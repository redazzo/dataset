import {Dataset, DataPump, FieldDescriptors} from "./dataset";
import {
    AuthChangeEvent,
    AuthError,
    createClient,
    Session,
    SupabaseClient,
    UserResponse
} from '@supabase/supabase-js';

export class SupabaseDataset extends Dataset {


    private selectSQL : {
        from: string,
        select: string,
        filter?: string
    }

    private upsertSQL : {

    }

    private rowCountLimit = 100;

    constructor(private table : string, fieldDescriptors : FieldDescriptors, private primaryKeys: { key: string, auto: boolean }[], private supabaseClient : SupabaseClient) {
        super(fieldDescriptors);

    }

    public init() {
        this.select = async () => {
            return await this.supabaseClient.from(this.table).select().limit(this.rowCountLimit);
        }
    }

    public set select( v: () => Promise<{ data, error }> ){

    }

    public set insert( v: () => { data, error } ) {

    }

    public set upsert( v: () => { data, error } ) {

    }

    public set delete( v: () => { data, error } ) {

    }

    public async load(){

    }

    public async write(){

    }


}
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