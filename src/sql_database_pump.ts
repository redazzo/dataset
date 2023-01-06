import {Dataset, PersistentDataPump} from "./dataset";
import {createClient } from '@supabase/supabase-js';

export class SQLDatabasePump implements PersistentDataPump {

    private readonly theSupabaseClient;

    public select : () => Promise<{ data, count, error? }>;

    private primaryKeys : string[];

    constructor(credentials: { url: string, key: string }) {
        this.theSupabaseClient = createClient(credentials.url, credentials.key);
    }



    public get supabaseClient(){
        return this.theSupabaseClient;
    }


    public async load(dataset: Dataset) {

        const { data, count, error } = await this.select();

        if (error != null) {

            console.log(error);
            return;
        }

        if (data == null) {
            return
        }

        for (let value of data) {

            let row = dataset.addRow();
            for (let fieldDescriptor of dataset.fieldDescriptors.values()) {

                row.setFieldValue(fieldDescriptor.name, value[fieldDescriptor.name]);

            }
        }
    }

    public async save(dataset: Dataset) {

    }
}