import {DataRow, Dataset, DatasetEvent, DatasetEventType, DatasetRow, PersistentDataPump} from "./dataset";
import {createClient} from '@supabase/supabase-js';
import {KeyedPersistentDataset} from "./persistent_dataset";

export enum PersistenceMode {
    BY_FIELD,
    BY_ROW,
    BY_DATASET
}

export enum SaveMode {
    OVERWRITE,
    OPTIMISTIC
}

export class SQLDatabasePump implements PersistentDataPump<KeyedPersistentDataset> {

    public Persistence_Mode = PersistenceMode.BY_FIELD;

    private readonly theSupabaseClient;
    private dataSet: Dataset;
    private keys : string[];
    private theSelectFunction : ( pump : SQLDatabasePump ) => Promise<{ data, count, error? }>;
    private theUpdateFunction : ( pump: SQLDatabasePump ) => Promise<{ data, status, statusText }>;

    private delete;
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

    public set update( updateFunction : ( t: SQLDatabasePump ) => Promise<{ data, status, statusText }>) {
        this.theUpdateFunction = updateFunction;
    }

    public get update() : ( t : SQLDatabasePump ) => Promise<{ data, status, statusText }> {
        return this.theUpdateFunction;
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

        let outerThis = this;

        if (this.dataSet == null || this.dataSet == undefined) {

            this.dataSet = dataset;

            if (dataset.source != null && dataset.source != undefined) {

                let tableName = dataset.source.tableName;
                this.keys = dataset.source.keys;

                if (this.select == null || this.select == undefined) {

                    this.select = ( aPump) => {

                        let selectedFields = aPump.buildSelectedFields();

                        let query = aPump.supabaseClient.from(tableName).select(selectedFields, { count: 'exact'});

                        // TODO Support paging

                        return query;
                    }

                }



                this.update = ( t : SQLDatabasePump ) => {

                    if (this.Persistence_Mode == PersistenceMode.BY_FIELD) {

                    }

                    let currentRow : DatasetRow = dataset.navigator().current().value;

                    let updatedFields : Object = {};

                    for (let field of currentRow.entries()){

                        if (field.isModified){
                            updatedFields[field.name] = field.value;
                        }
                    }

                    let updateQuery = t.supabaseClient.from(tableName).update(updatedFields);


                    for (let key of this.keys){

                        let theField = dataset.getField(key);

                        updateQuery = updateQuery.eq(theField.name, theField.value)
                    }

                    updateQuery = updateQuery.select();

                    return updateQuery;

                }



            }


            this.dataSet.subscribe(async (event: DatasetEvent<Dataset, DataRow>) => {

                let affectedRow = event.detail;
                let dataset = event.source;



                //let keyValues : object[] = [];

                //for (let key of outerThis.keys){
                //    keyValues[key] = affectedRow.getValue(key);
                //}

                if (event.eventType == DatasetEventType.ROW_DELETED) {
                    // Delete row from DB
                }

                if (event.eventType == DatasetEventType.ROW_INSERTED) {
                    // Insert row into DB
                }

                if (event.eventType == DatasetEventType.ROW_UPDATED) {

                    if (this.Persistence_Mode == PersistenceMode.BY_FIELD) {
                        const { data, status, statusText } = await outerThis.update(this);
                        console.log("Updated " + JSON.stringify(data) + ": STATUS - " + status);
                    }
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