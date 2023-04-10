import {DataRow, Dataset, DatasetEvent, DatasetEventType, DatasetRow, PersistentDataPump} from "./dataset";
import {createClient} from '@supabase/supabase-js';
import {KeyedPersistentDataset} from "./persistent_dataset";
import Any = jasmine.Any;

export enum PersistenceMode {
    BY_FIELD,
    BY_ROW,
    BY_DATASET
}

export enum SaveMode {
    OVERWRITE,
    OPTIMISTIC
}

/**
 * The SupabaseDataPump is a PersistentDataPump that can be used to load and save data from a Supabase database.
 */
export class SupabaseDataPump implements PersistentDataPump<KeyedPersistentDataset> {

    public Persistence_Mode = PersistenceMode.BY_FIELD;

    private readonly theSupabaseClient;
    private dataSet: Dataset;
    private keys: string[];
    private selectFunction: (pump: SupabaseDataPump) => Promise<{ data, count, error? }>;
    private updateFunction: (pump: SupabaseDataPump) => Promise<{ data, status, statusText }>;
    private deleteFunction: (pump: SupabaseDataPump) => Promise<{ data, status, statusText }>;
    private insertFunction: (pump: SupabaseDataPump) => Promise<{ data, status, statusText }>;


    constructor(credentials: { url: string, key: string }, public useTableSource: boolean) {
        this.theSupabaseClient = createClient(credentials.url, credentials.key);


    }

    public get supabaseClient() {
        return this.theSupabaseClient;
    }

    public set select(selectFunction: (t: SupabaseDataPump) => Promise<{ data, count, error? }>) {
        this.selectFunction = selectFunction;
    }

    public get select(): ((t: SupabaseDataPump) => Promise<{ data, count, error? }>) {
        return this.selectFunction;
    }

    public set update(updateFunction: (t: SupabaseDataPump) => Promise<{ data, status, statusText }>) {
        this.updateFunction = updateFunction;
    }

    public get update(): (t: SupabaseDataPump) => Promise<{ data, status, statusText }> {
        return this.updateFunction;
    }


    public async load(dataset: KeyedPersistentDataset) {

        await dataset.clear();
        this.initialisePump(dataset);

        const {data, count, error} = await this.selectFunction(this);

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

        if (dataset == null || dataset == undefined) {
            throw new Error("Dataset must be specified");
        }

        if (dataset.source == null || dataset.source == undefined) {
            throw new Error("Dataset source must be specified");
        }


        this.dataSet = dataset;

        let tableName = dataset.source.tableName;
        this.keys = dataset.source.keys;

        if (this.select == null || this.select == undefined) {

            this.select = (aPump) => {

                let selectedFields = aPump.buildSelectedFields();

                let query = aPump.supabaseClient.from(tableName).select(selectedFields, {count: 'exact'});

                // TODO Support paging

                return query;
            }

        }


        // The update function finds the fields that have been modified and updates them
        this.update = (t: SupabaseDataPump) => {

            console.log("Updating row");

            if (this.Persistence_Mode == PersistenceMode.BY_FIELD) {

            }

            // TODO Support optimistic locking and other modes
            let currentRow: DatasetRow = dataset.navigator().current().value;

            let updatedFields: Object = {};

            for (let field of currentRow.entries()) {

                if (field.isModified) {
                    updatedFields[field.name] = field.value;
                }
            }

            let updateQuery = t.supabaseClient.from(tableName).update(updatedFields);

            for (let key of this.keys) {

                let theField = dataset.getField(key);

                updateQuery = updateQuery.eq(theField.name, theField.value)
            }

            updateQuery = updateQuery.select();

            return updateQuery;

        }


        // outer is used to get around the fact that the "this" keyword will likely return the wrong instance
        // when used in the async function
        let outerThis = this;
        this.dataSet.subscribe(async (event: DatasetEvent<Dataset, DataRow>) => {

            let affectedRow = event.detail;
            let dataset = event.source;

            // If the row has been deleted, then we need to delete it from the DB
            if (event.eventType == DatasetEventType.ROW_DELETED) {
                // Delete row from DB
            }

            // If the row has been inserted, then we need to insert it into the DB
            if (event.eventType == DatasetEventType.ROW_INSERTED) {
                // Insert row into DB
            }

            // If the row has been updated, then we need to update the DB
            if (event.eventType == DatasetEventType.ROW_UPDATED) {

                if (this.Persistence_Mode == PersistenceMode.BY_FIELD) {
                    const {data, status, statusText} = await outerThis.update(this);
                    console.log("Updated " + JSON.stringify(data) + ": STATUS - " + status);
                }
            }
        });

    }

    //
    private buildSelectedFields(): string {

        let selectedFields = "";

        let keys = this.dataSet.fieldDescriptors.keys();
        let noKeys = this.dataSet.fieldDescriptors.size;

        let noKeysDone = 0;
        for (let key of keys) {
            selectedFields = selectedFields.concat(key);
            noKeysDone++;
            if (noKeysDone == noKeys) break;
            selectedFields = selectedFields.concat(",");
        }

        return selectedFields;
    }

    public async save(dataset: KeyedPersistentDataset) {

            /*let navigator = dataset.navigator();

            let row: DatasetRow;

            while (navigator.next()) {

                row = navigator.current().value;

                if (row.isModified) {

                    if (row.isNew) {

                        let insertQuery = this.supabaseClient.from(dataset.source.tableName).insert(row.entries());

                        const {data, status, statusText} = await insertQuery;

                        console.log("Inserted " + JSON.stringify(data) + ": STATUS - " + status);

                    } else {

                        const {data, status, statusText} = await this.update(this);

                        console.log("Updated " + JSON.stringify(data) + ": STATUS - " + status);

                    }
                }
            }*/
    }
}