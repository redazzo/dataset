import { SupabaseDataPump} from "../supabase_database_pump";
import {KeyedPersistentDataset, ReactiveWriteMode} from "../persistent_dataset";
import {createClient} from '@supabase/supabase-js';
import {DatasetRow, FieldType, GlobalMutex} from "../dataset";

const credentials = {
    url: 'https://erhnfxdfmdtqjchmofge.supabase.co',
    key: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVyaG5meGRmbWR0cWpjaG1vZmdlIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NzkxMzM4MTYsImV4cCI6MTk5NDcwOTgxNn0.oOUlMjFuJntHyYS5Yviq1ljHlT7q4_ra6-_nDAOxHps'
}

const TABLE_NAME = "persistence_test"
const MAKE = "make"
const MODEL = "model"
const YEAR = "year"
const ID = "id"


const test_data = {
    1: {
        make: "Chevrolet",
        model: "Camaro",
        year: 1967
    },
    2: {
        make: "Ford",
        model: "Mustang",
        year: 1965
    },
    3: {
        make: "Dodge",
        model: "Charger",
        year: 1969
    }

}

const test_data_2 = {
    1: {
        make: "Chev",
        model: "Cam",
        year: 1999

    }
}

let firstId = 1;

const supabaseClient = createClient(credentials.url, credentials.key);

const sleep = (ms) => new Promise(r => setTimeout(r, ms));


beforeEach(async () => {
    console.log('1 - beforeEach')

    // Delete all rows
    await supabaseClient.from(TABLE_NAME).delete().neq("id","-1").then((result) => {
        console.log(result);
    });

    // Insert the test data
    let isFirstResult = false;
    for (let i = 1; i <= 3; i++) {

        await supabaseClient.from(TABLE_NAME).insert([ test_data[i] ]).then((result) => {
            console.log(result);
        });

        if (!isFirstResult) {
            await supabaseClient.from(TABLE_NAME).select().then((result) => {
                console.log(result);
                isFirstResult = true;

                firstId = parseInt(result.data[0].id);
            });
        }
    }

    await sleep(1000);
});

afterEach(async () => {
        console.log('1 - afterEach');
    await sleep(1000);
});

/**
 * Create a dataset for use during testing
 */
function createDataset(reactiveWriteMode : ReactiveWriteMode) {

    let dataPump = new SupabaseDataPump(credentials);
    dataPump.Reactive_Write_Mode = reactiveWriteMode;

    const columnTypes = [
        {
            name: MAKE,
            type: FieldType.STRING
        },
        {
            name: MODEL,
            type: FieldType.STRING
        },
        {
            name: YEAR,
            type: FieldType.INTEGER
        },
        {
            name: ID,
            type: FieldType.INTEGER
        }
    ];

    let dataset = new KeyedPersistentDataset(columnTypes, dataPump, {tableName: TABLE_NAME, keys: [ID]});
    return dataset;
}

/**
 * Test that the dataset can be loaded from the database
 */
test('Load Dataset', async () => {

    let dataset = createDataset(ReactiveWriteMode.ENABLED);

    // Ensure that the dataset is empty after creation
    expect(dataset.rowCount).toBe(0);

    await dataset.load();

    // Ensure that the dataset has been populated
    expect(dataset.rowCount).toBe(3);

});

/**
 * Test that the dataset can be updated and reloaded.
 */
test("Update and Reset", async () => {

    // Cycle through the dataset and update and reset each row
    let dataset = createDataset(ReactiveWriteMode.ENABLED);
    await dataset.load();

    for (let i = 1; i <= 3; i++) {



        let rowId = firstId + i - 1

        // Find the row with id = rowId
        let result = dataset.navigator().moveToFind(new Map([[ID, rowId]]));

        expect(result.next().value).not.toBe(undefined);

        // Find the original values
        let originalValue = dataset.getField(MAKE).value;
        let id = dataset.getField(ID).value;

        // Ensure that the values are correct
        expect(originalValue).toBe(test_data[i].make);
        expect(id).toBe(rowId);

        // Change the value
        dataset.setFieldValue(MAKE, "LALALAND");

        // Reload the dataset
        await dataset.load();



        // Find the row with id = rowId again
        result = dataset.navigator().moveToFind(new Map([[ID, rowId]]));
        expect(result.next().value).not.toBe(undefined);

        // Find the new values
        let newValue = dataset.getField(MAKE).value;

        // Ensure that the value has been changed
        expect(newValue).toBe("LALALAND");

        // Reset the value
        dataset.setFieldValue(MAKE, test_data[i].make);

        // Reload the dataset
        await dataset.load();

        // Find the row with id = i again
        result = dataset.navigator().moveToFind(new Map([[ID, rowId]]));
        expect(result.next().value).not.toBe(undefined);

        // Find the new values
        newValue = dataset.getField(MAKE).value;

        // Ensure that the value has been changed back
        expect(newValue).toBe(test_data[i].make);

    }

    },20000);

/**
 * Test that the dataset can be loaded, updated at several locations, saved, and reloaded.
 */
test("Update and Save", async () => {

    let dataset = createDataset(ReactiveWriteMode.ENABLED);
    await dataset.load();

    expect(dataset.rowCount).toBe(3);

    let result = dataset.navigator().moveToFind(new Map([[ID, firstId]]));

    let firstRow = result.next().value;
    expect(firstRow).not.toBe(undefined);

    let datasetRowId = firstRow.id;
    dataset.deleteRow(datasetRowId);

    expect(dataset.rowCount).toBe(2);

    dataset = createDataset(ReactiveWriteMode.ENABLED);
    await dataset.load();

    expect(dataset.rowCount).toBe(2);

    // cnt is 1 because we deleted the first row
    let cnt = 1;
    for (let row of dataset.navigator()) {
        expect((row.getField(ID).value)).toBe(firstId + cnt++);
    }

});

/**
 * Test manual row insertion by directly using the supabase client.
 */
test("Manual Row Insertion", async () => {

        //await sleep(1000);
        let dataset = createDataset(ReactiveWriteMode.ENABLED);
        await dataset.load();

        expect(dataset.rowCount).toBe(3);

        // Insert a row manually
        await supabaseClient.from(TABLE_NAME).insert({
            make: "",
            model: "",
            year: 0,
        }).then((result) => {
            console.log(result);
        });

        // Wait for database to be updated
        await sleep(1000);

        dataset = createDataset(ReactiveWriteMode.ENABLED);
        await dataset.load();

        expect(dataset.rowCount).toBe(4);

        // cnt is 0 because we didn't delete any rows, so we can start at 0
        let cnt = 0;
        for (let row of dataset.navigator()) {
            expect((row.getField(ID).value)).toBe(firstId + cnt++);
        }

});


/**
 * Test that a row can be added to the dataset and saved.
 */
test("Add Row", async () => {

        let dataset = createDataset(ReactiveWriteMode.ENABLED);
        await dataset.load();

        expect(dataset.rowCount).toBe(3);

        let newRow = dataset.addRow();

        newRow.setFieldValue(MAKE, "LALALAND");
        newRow.setFieldValue(MODEL, "LALALAND");
        newRow.setFieldValue(YEAR, "1965");

        expect(dataset.rowCount).toBe(4);

        dataset = createDataset(ReactiveWriteMode.ENABLED);
        await dataset.load();

        expect(dataset.rowCount).toBe(4);

        let cnt = 0;
        for (let row of dataset.navigator()) {
            expect((row.getField(ID).value)).toBe(firstId + cnt++);
        }

});

class Waiter {
    private timeout: any

    constructor() {
        this.waitLoop()
    }

    private waitLoop(): void {
        this.timeout = setTimeout(() => {
            this.waitLoop()
        }, 100 * 1000)
    }

    okToQuit(): void {
        clearTimeout(this.timeout)
    }
}
