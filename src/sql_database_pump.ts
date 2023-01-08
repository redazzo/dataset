import {DataRow, Dataset, DatasetEvent, DatasetEventType, PersistentDataPump} from "./dataset";
import {createClient} from '@supabase/supabase-js';
import {Comparator, KeyedPersistentDataset} from "./persistent_dataset";

export class SQLDatabasePump implements PersistentDataPump<KeyedPersistentDataset> {

    private readonly theSupabaseClient;
    private dataSet: Dataset;
    private theSelectFunction : ( t: SQLDatabasePump ) => Promise<{data, count, error? }>;

    private delete;
    public update : () => {data, error?};
    private insert;

    //public select : (from: string, select: string, keys?: string[], filter?: string[]) => { data, count, error? };

    //public delete : ( filter? : string[]) => Promise<{ data, count, error?}>;

    //public update : ( filter? : string[]) => Promise<{ data, error? }>;

    //public insert : ( values? : string[]) => Promise<{ data, error?}>;


    constructor(credentials: { url: string, key: string }, public useTableSource : boolean ) {
        this.theSupabaseClient = createClient(credentials.url, credentials.key);
    }

    public get supabaseClient(){
        return this.theSupabaseClient;
    }

    public set select( selectFunction : ( t: SQLDatabasePump ) => Promise<{ data, count, error? }>) {
        this.theSelectFunction = selectFunction;
    }

    public get select() : ( t: SQLDatabasePump ) => Promise<{ data, count, error? }> {
        return this.theSelectFunction;
    }


    public async load(dataset: KeyedPersistentDataset) {

        this.initialisePump(dataset);

        const { data, count, error } = await this.select(this);

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

    private initialisePump(dataset: KeyedPersistentDataset) {

        if (this.dataSet == null || this.dataSet == undefined) {

            this.dataSet = dataset;

            if (dataset.source != null && dataset.source != undefined) {

                let tableName = dataset.source.tableName;

                 this.select = async ( t : SQLDatabasePump ) => {

                    let selectedFields = t.buildSelectedFields();

                    return t.supabaseClient.from(tableName).select(selectedFields, {count: 'exact'});

                }

            }


            this.dataSet.subscribe((event: DatasetEvent<Dataset, DataRow>) => {

                let affectedRow = event.detail;

                let keyValues : object[] = [];

                //for (let key of this.primaryKeys){
                //    keyValues[key] = affectedRow.getValue(key);
                //}

                if (event.eventType == DatasetEventType.ROW_DELETED) {
                    // Delete row from DB
                }

                if (event.eventType == DatasetEventType.ROW_INSERTED) {
                    // Insert row into DB
                }

                if (event.eventType == DatasetEventType.ROW_UPDATED) {

                    // Find the updated fields, amd write this back to the DB

                }
            });
        }
    }

    private buildSelectedFields(): string {

        let selectedFields = "";

        let keys = this.dataSet.fieldDescriptors.keys();
        let noKeys = this.dataSet.fieldDescriptors.size;

        let noKeysDone = 0;
        for (let key of keys){
            selectedFields = selectedFields.concat(key);
            noKeysDone++;
            if (noKeysDone == noKeys) break;
            selectedFields = selectedFields.concat(",");
        }

        return selectedFields;
    }

    public async save(dataset: KeyedPersistentDataset) {

    }
}