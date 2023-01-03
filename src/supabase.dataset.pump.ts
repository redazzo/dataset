import {Dataset, DataPump} from "./dataset";
import {
    AuthChangeEvent,
    AuthError,
    createClient,
    Session,
    SupabaseClient,
    UserResponse
} from '@supabase/supabase-js';

export class SupabaseDatasetPump implements DataPump {

    private readonly supabaseClient;

    private sqlParameters : {
        from: string,
        select: string,
        filter?: string
    }


    constructor( credentials : { url : string, key : string }){
        this.supabaseClient = createClient(credentials.url, credentials.key);
    }

    public setSQLParameters(sqlParameters : {
        from: string,
        select: string,
        filter?: string
    }){
        this.sqlParameters = sqlParameters;
    }

    public async load(dataset: Dataset) {

        const { data, error, count } = await this.supabaseClient
            .from(this.sqlParameters.from)
            .select(this.sqlParameters.select, { count: 'exact'});
            //.filter(this.sqlParameters?.filter);

        if (error != null){

            console.log(error);
            return;
        }

        if (data == null) {
            return
        };

        let index = 0;
        for (let value of data){

            let row = dataset.addRow();
            for (let fieldDescriptor of dataset.fieldDescriptors.values()){

                row.setFieldValue(fieldDescriptor.name, data[index][fieldDescriptor.name]);

            }

            index++;
        }


    }
}