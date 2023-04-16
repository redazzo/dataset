import {DataRow, Dataset, DatasetEvent, DatasetEventType, DatasetRow, LoadStatus, PersistentDataPump} from "./dataset";
import {createClient} from '@supabase/supabase-js';
import {KeyedPersistentDataset} from "./persistent_dataset";
import Any = jasmine.Any;

/**
 * PersistenceMode determines whether only modified fields and rows are written back to the database, or all fields are written back.
 */
export enum PersistenceMode {

    // Rows with modified fields are updated, and only the modified fields are written back
    BY_FIELD,

    // Rows with modified fields are updated, and all fields are written back
    BY_ROW,

    // All rows are updated and/or inserted, and all fields are written back
    BY_DATASET
}

/**
 * ReactiveWriteMode determines whether a field change will trigger a write to the database.
 */
export enum ReactiveWriteMode {
    DISABLED    = 0,   // Reactive writes are disabled
    ENABLED     = 1,   // Reactive writes are enabled
}

/**
 * AUTO_KEY determines whether the database will generate a key for a new row.
 */
export enum AUTO_KEY {
    FALSE,
    TRUE
}

/**
 * SaveMode determines whether a save operation will overwrite existing data, or will fail if the data has been modified since it was loaded.
 */
export enum SaveMode {
    OVERWRITE,
    OPTIMISTIC
}

/**
 * The SupabaseDataPump is a PersistentDataPump that can be used to load and save data from a Supabase database.
 */
export class SupabaseDataPump implements PersistentDataPump<KeyedPersistentDataset> {

    public Reactive_Write_Mode = ReactiveWriteMode.ENABLED;

    public Persistence_Mode = PersistenceMode.BY_FIELD;
    public Auto_Key = AUTO_KEY.TRUE;


    private readonly theSupabaseClient;
    private dataSet: Dataset;
    private keys: string[];
    private theTableName: string;
    private selectFunction: (pump: SupabaseDataPump) => Promise<{ data, count, error? }>;
    private updateFunction: (pump: SupabaseDataPump) => Promise<{ data, status, statusText }>;
    private deleteFunction: (pump: SupabaseDataPump) => Promise<{ data, status, statusText }>;
    private insertFunction: (pump: SupabaseDataPump) => Promise<{ data, status, statusText }>;


    constructor(credentials: { url: string, key: string }) {
        this.theSupabaseClient = createClient(credentials.url, credentials.key);


    }

    public get tableName(): string {
        return this.theTableName;
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

    public get insert(): (t: SupabaseDataPump) => Promise<{ data, status, statusText }> {
        return this.insertFunction;
    }

    public set insert(insertFunction: (t: SupabaseDataPump) => Promise<{ data, status, statusText }>) {
        this.insertFunction = insertFunction;
    }

    public get delete(): (t: SupabaseDataPump) => Promise<{ data, status, statusText }> {
        return this.deleteFunction;
    }

    public set delete(deleteFunction: (t: SupabaseDataPump) => Promise<{ data, status, statusText }>) {
        this.deleteFunction = deleteFunction;
    }


    public async load(dataset: KeyedPersistentDataset) : Promise<LoadStatus> {

        await dataset.clear();
        this.initialisePump(dataset);

        const {data, count, error} = await this.selectFunction(this);

        if (error != null) {

            console.log(error);
            return LoadStatus.FAILURE;
        }

        if (data == null) {
            return LoadStatus.SUCCESS;
        }


        for (let value of data) {

            let row = dataset.addRow();
            for (let fieldDescriptor of dataset.fieldDescriptors.values()) {

                row.setFieldValue(fieldDescriptor.name, value[fieldDescriptor.name]);

            }
        }

        return LoadStatus.SUCCESS;
    }

    private initialisePump(dataset: KeyedPersistentDataset) {

        if (dataset == null || dataset == undefined) {
            throw new Error("Dataset must be specified");
        }

        if (dataset.source == null || dataset.source == undefined) {
            throw new Error("Dataset source must be specified");
        }


        this.dataSet = dataset;

        this.theTableName = dataset.source.tableName;
        this.keys = dataset.source.keys;

        if (this.select == null || this.select == undefined) {

            this.select = (aPump) => {

                let selectedFields = aPump.buildSelectedFields();

                let query = aPump.supabaseClient.from(this.theTableName).select(selectedFields, {count: 'exact'});

                // TODO Support paging

                return query;
            }

        }

        // Prepare the update function
        this.update = (t: SupabaseDataPump) => {

            console.log("Updating row");

            // TODO Support optimistic locking and other modes
            let currentRow: DatasetRow = dataset.navigator().current().value;

            let queryFields: Object = {};

            for (let field of currentRow.entries()) {

                switch (this.Persistence_Mode) {
                    case PersistenceMode.BY_FIELD: {
                        if (field.modified) {
                            queryFields[field.name] = field.value;
                        }
                        break;
                    }

                    default: {
                        queryFields[field.name] = field.value;
                        break;
                    }
                }
            }

            let updateQuery = t.supabaseClient.from(this.theTableName).update(queryFields);

            for (let key of this.keys) {

                let theField = dataset.getField(key);

                updateQuery = updateQuery.eq(theField.name, theField.value)
            }

            updateQuery = updateQuery.select();

            return updateQuery;

        }

        // Prepare the insert function
        this.insert = (t: SupabaseDataPump) => {

                console.log("Inserting row");

                // TODO Support optimistic locking and other modes
                let currentRow: DatasetRow = dataset.navigator().current().value;

                let fields: Object = {};

                for (let field of currentRow.entries()) {


                    fields[field.name] = field.value;
                }

                let updateQuery = t.supabaseClient.from(this.theTableName).insert(fields);

                return updateQuery;

        }

        this.delete = (t: SupabaseDataPump) => {

                console.log("Deleting row");

                // TODO Support optimistic locking and other modes
                let deletedRow = dataset.deletedRows[0];

                let deleteQuery = t.supabaseClient.from(this.theTableName).delete();

                for (let key of this.keys) {

                    let theField = deletedRow.getField(key);

                    deleteQuery = deleteQuery.eq(theField.name, theField.value)
                }

                deleteQuery = deleteQuery.select();

                dataset.clearDeletedRows();

                return deleteQuery;

        }



        // Subscribes to the dataset so that we can update the DB when the dataset is updated in reactive mode

        // outer is used to get around the fact that the "this" keyword returns the wrong instance
        // when used in the async function
        let outerThis = this;
        this.dataSet.subscribe(async (event: DatasetEvent<Dataset, DataRow>) => {

            let affectedRow = event.detail;
            let dataset = event.source;

            // If the row has been deleted, then we need to delete it from the DB
            if (event.eventType == DatasetEventType.ROW_DELETED && outerThis.Reactive_Write_Mode == ReactiveWriteMode.ENABLED) {
                // Delete row from DB if reactive write mode is enabled
                if (outerThis.Reactive_Write_Mode == ReactiveWriteMode.ENABLED) {
                    const {data, status, statusText} = await outerThis.delete(this);
                    console.log("Deleted " + JSON.stringify(data) + ": STATUS - " + status);
                }
            }

            // If the row has been inserted, then we need to insert it into the DB
            if (event.eventType == DatasetEventType.ROW_INSERTED) {
                // Insert row into DB if reactive write mode is enabled
                if (outerThis.Persistence_Mode == PersistenceMode.BY_FIELD && outerThis.Reactive_Write_Mode == ReactiveWriteMode.ENABLED) {
                    const {data, status, statusText} = await outerThis.insert(this);
                    console.log("Inserted " + JSON.stringify(data) + ": STATUS - " + status);
                }
                affectedRow.resetModified();
            }

            // If the row has been updated, then we need to update the DB
            if (event.eventType == DatasetEventType.ROW_UPDATED) {

                // Update row in DB if reactive write mode is enabled
                if (outerThis.Persistence_Mode == PersistenceMode.BY_FIELD && outerThis.Reactive_Write_Mode == ReactiveWriteMode.ENABLED) {
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

        // TODO Support optimistic locking and other modes
        // TODO Handle new rows

        switch (this.Persistence_Mode) {
            case PersistenceMode.BY_DATASET: {

                // Iterate through all the rows, and update all fields
                for (let row of dataset.navigator()) {
                    let queryFields: Object = {};
                    for (let field of row.entries()) {
                        queryFields[field.name] = field.value;
                    }
                    let updateQuery = this.supabaseClient.from(this.theTableName).update(queryFields);
                    for (let key of this.keys) {
                        let theField = dataset.getField(key);
                        updateQuery = updateQuery.eq(theField.name, theField.value)
                    }
                    updateQuery = updateQuery.select();
                    const {data, status, statusText} = await updateQuery;
                    console.log("Updated " + JSON.stringify(data) + ": STATUS - " + status);
                }
                break;
            }
            case PersistenceMode.BY_ROW: {
                // Iterate through all the updated rows, and update all fields in each row
                for (let row of dataset.navigator()) {
                    if (row.modified) {
                        let queryFields: Object = {};
                        for (let field of row.entries()) {
                            queryFields[field.name] = field.value;
                        }
                        let updateQuery = this.supabaseClient.from(this.theTableName).update(queryFields);
                        for (let key of this.keys) {
                            let theField = dataset.getField(key);
                            updateQuery = updateQuery.eq(theField.name, theField.value)
                        }
                        updateQuery = updateQuery.select();
                        const {data, status, statusText} = await updateQuery;
                        console.log("Updated " + JSON.stringify(data) + ": STATUS - " + status);
                    }
                }
                break;
            }
            case PersistenceMode.BY_FIELD: {
                // Iterate through all the updates rows, and update only the modified fields in each row
                for (let row of dataset.navigator()) {
                    if (row.modified) {
                        let queryFields: Object = {};
                        for (let field of row.entries()) {
                            if (field.modified) {
                                queryFields[field.name] = field.value;
                            }
                        }
                        let updateQuery = this.supabaseClient.from(this.theTableName).update(queryFields);
                        for (let key of this.keys) {
                            let theField = dataset.getField(key);
                            updateQuery = updateQuery.eq(theField.name, theField.value)
                        }
                        updateQuery = updateQuery.select();
                        const {data, status, statusText} = await updateQuery;
                        console.log("Updated " + JSON.stringify(data) + ": STATUS - " + status);
                    }
                }
            }
        }

    }
}