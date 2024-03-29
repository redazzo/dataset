import {
    DataRow,
    Dataset,
    DatasetEvent,
    DatasetEventType,
    DatasetRow,
    FieldType, GlobalMutex,
    LoadStatus,
    PersistentDataPump
} from "./dataset";
import {createClient} from '@supabase/supabase-js';
import {DB_AUTO_KEY, KeyedPersistentDataset, PersistenceMode, ReactiveWriteMode} from "./persistent_dataset";



/**
 * The SupabaseDataPump is a PersistentDataPump that can be used to load and save data from a Supabase database.
 */
export class SupabaseDataPump implements PersistentDataPump<KeyedPersistentDataset> {

    public Reactive_Write_Mode = ReactiveWriteMode.ENABLED;

    public Persistence_Mode = PersistenceMode.BY_FIELD;
    public Auto_Key = DB_AUTO_KEY.TRUE;


    private readonly theSupabaseClient;
    private dataSet: Dataset;
    private keys: string[];
    private theTableName: string;
    private filter = new Map<string, any>();

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

    public setLoadFilter(filter: Map<string, any>) {
        // TODO: test to ensure that the filter is valid
        this.filter = filter;
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


        //let result = await GlobalMutex.runExclusive<LoadStatus>(async () : Promise<LoadStatus> => {
            await dataset.clear();
            this.initialisePump(dataset);

            let loadStatus = LoadStatus.SUCCESS;

            const {data, count, error} = await this.selectFunction(this);
            if (error != null) {

                console.log(error);
                loadStatus = LoadStatus.FAILURE;
            }

            if (data == null) {
                loadStatus = LoadStatus.FAILURE;
            }


            for (let value of data) {

                let row = dataset.addRow();
                for (let fieldDescriptor of dataset.fieldDescriptors.values()) {

                    row.setFieldValue(fieldDescriptor.name, value[fieldDescriptor.name]);

                }
            }

            return loadStatus;
        //});

        //return result;
    }

    private initialisePump(dataset: KeyedPersistentDataset) {

        if (dataset == null) {
            throw new Error("Dataset must be specified");
        }

        if (dataset.source == null) {
            throw new Error("Dataset source must be specified");
        }

        this.dataSet = dataset;

        this.theTableName = dataset.source.tableName;
        this.keys = dataset.source.keys;

        this.buildSelectFunction();
        this.buildUpdateFunction(dataset);
        this.buildInsertFunction(dataset);
        this.buildDeleteFunction(dataset);

        this.subscribeToDatasetUpdateEvents();

    }

    // Subscribes to the dataset so that we can update the DB when the dataset is updated in reactive mode
    private subscribeToDatasetUpdateEvents() {
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
                    await GlobalMutex.runExclusive(async () => {
                        const {data, status, statusText} = await outerThis.delete(this);
                        console.log("Deleted " + JSON.stringify(data) + ": STATUS - " + status);
                    });
                }
            }

            // If the row has been inserted, then we need to insert it into the DB
            if (event.eventType == DatasetEventType.ROW_INSERTED) {
                // Insert row into DB if reactive write mode is enabled
                // Note this only works if the  database generates the key, and there are no fields that are defined as NOT NULL in the DB
                if (outerThis.Persistence_Mode == PersistenceMode.BY_FIELD && outerThis.Reactive_Write_Mode == ReactiveWriteMode.ENABLED) {
                    await GlobalMutex.runExclusive(async () => {
                        const {data, status, statusText} = await outerThis.insert(this);

                        // Update the key field with the value returned from the DB
                        outerThis.dataSet.quiet = true;
                        if (outerThis.Auto_Key == DB_AUTO_KEY.TRUE) {
                            this.keys.forEach((key) => {
                                affectedRow.setFieldValue(key, data[0][key].toString());
                            });
                        }
                        outerThis.dataSet.quiet = false;
                        console.log("Inserted " + JSON.stringify(data) + ": STATUS - " + status);
                    });

                }
                affectedRow.resetModified();
            }

            // If the row has been updated, then we need to update the DB
            if (event.eventType == DatasetEventType.ROW_UPDATED) {

                // Update row in DB if reactive write mode is enabled
                if (outerThis.Persistence_Mode == PersistenceMode.BY_FIELD && outerThis.Reactive_Write_Mode == ReactiveWriteMode.ENABLED) {
                    await GlobalMutex.runExclusive(async () => {
                        const {data, status, statusText} = await outerThis.update(this);
                        console.log("Updated " + JSON.stringify(data) + ": STATUS - " + status);
                    });

                }
            }
        });
    }

    private buildDeleteFunction(dataset: KeyedPersistentDataset) {
        if (this.delete == null) {

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

        }
    }

    private buildInsertFunction(dataset: KeyedPersistentDataset) {
        if (this.insert == null) {
            this.insert = (t: SupabaseDataPump) => {

                console.log("Inserting row");

                // TODO Support optimistic locking and other modes
                let currentRow: DatasetRow = dataset.navigator().current().value;

                let fields: Object = {};

                for (let field of currentRow.entries()) {

                    let addField = true;
                    if (dataset.dbAutoKey) {
                        this.keys.forEach(key => {
                            if (field.name == key) {
                                addField = false;
                                return;
                            }
                        })
                    }

                    if (addField) {

                        let value = undefined;

                        switch (field.type) {
                            case FieldType.FLOAT:
                                value = parseFloat(field.value);
                                break;
                            case FieldType.BOOLEAN:
                                value = field.value == "true";
                                break;
                            case FieldType.DATE:
                                value = new Date(field.value);
                                break;
                            case FieldType.STRING:
                                value = field.value;
                                break;
                            case FieldType.INTEGER:
                                value = parseInt(field.value);
                                break;
                        }

                        fields[field.name] = value;
                    }
                }

                let updateQuery = t.supabaseClient.from(this.theTableName).insert(fields).select();

                this.resetModified(currentRow);

                return updateQuery;

            }

        }
    }

    private buildUpdateFunction(dataset: KeyedPersistentDataset) {
        if (this.update == null) {

            this.update = (pump: SupabaseDataPump) => {

                console.log("Updating row");

                // TODO Support optimistic locking and other modes
                let currentRow: DatasetRow = dataset.navigator().current().value;

                let queryFields = this.buildQueryFields(currentRow);

                let updateQuery = pump.supabaseClient.from(this.theTableName).update(queryFields);

                for (let key of this.keys) {

                    let theField = dataset.getField(key);

                    updateQuery = updateQuery.eq(theField.name, theField.value)
                }

                updateQuery = updateQuery.select();

                this.resetModified(currentRow);

                return updateQuery;

            }

        }
    }

    private buildSelectFunction() {
        if (this.select == null) {

            this.select = (aPump) => {

                let selectedFields = aPump.buildSelectedFields();

                let filter = this.buildFilter();

                let query = null;

                if (filter != null){
                    query = aPump.supabaseClient.from(this.theTableName).select(selectedFields, {count: 'exact'}).match(filter);
                } else {
                    query = aPump.supabaseClient.from(this.theTableName).select(selectedFields, {count: 'exact'});
                }
                // TODO Support paging

                return query;
            }

        }
    }

    private buildFilter() : {} {
        if (this.filter.size == 0) {
            return null;
        }

        let filter = {};

        for (let item of this.filter) {
            filter[item[0]] = item[1];
        }

        console.log(JSON.stringify(filter));
        return filter;
    }

    private buildQueryFields(currentRow: DatasetRow) {
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
        return queryFields;
    }

    private resetModified(currentRow: DatasetRow) {
        for (let field of currentRow.entries()) {
            field.resetModified();
        }
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
                    await GlobalMutex.runExclusive(async () => {
                        const {data, status, statusText} = await updateQuery.select();
                        console.log("Updated " + JSON.stringify(data) + ": STATUS - " + status);
                    });
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
                        await GlobalMutex.runExclusive(async () => {
                            const {data, status, statusText} = await updateQuery.select();
                            console.log("Updated " + JSON.stringify(data) + ": STATUS - " + status);
                        });
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
                        await GlobalMutex.runExclusive(async () => {
                            const {data, status, statusText} = await updateQuery.select();
                            console.log("Updated " + JSON.stringify(data) + ": STATUS - " + status);
                        });
                    }
                }
            }
        }

    }
}