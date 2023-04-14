import {SupabaseDataPump} from "../supabase_database_pump";
import {KeyedPersistentDataset} from "../persistent_dataset";
import {FieldType} from "../dataset";
import * as assert from "assert";

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

const sleep = (ms) => new Promise(r => setTimeout(r, ms));


function createDataset() {

    //        .eq('id', 1).select();
    //}
    //.filter(this.sqlParameters?.filter);

    let supabaseDatasetPump = new SupabaseDataPump(credentials);

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

    let dataset = new KeyedPersistentDataset(columnTypes, supabaseDatasetPump, {tableName: TABLE_NAME, keys: [ID]});
    return dataset;
}

/**
 * Test that the dataset can be loaded from the database
 */
test('Load Dataset', async () => {

    let dataset = createDataset();

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

    for (let i = 1; i <= 3; i++) {

        let dataset = createDataset();
        await dataset.load();

        // Find the row with id = i
        dataset.navigator().moveToFind(new Map([[ID, i]]));

        // Find the original values
        let originalValue = dataset.getField(MAKE).value;
        let id = dataset.getField(ID).value;

        // Ensure that the values are correct
        expect(originalValue).toBe(test_data[i].make);
        expect(id).toBe(i);

        // Change the value
        dataset.setFieldValue(MAKE, "LALALAND");

        // Wait for database to be updated
        await sleep(1000);

        // Reload the dataset
        await dataset.load();

        // Find the row with id = i again
        dataset.navigator().moveToFind(new Map([[ID, i]]));

        // Find the new values
        let newValue = dataset.getField(MAKE).value;

        // Ensure that the value has been changed
        expect(newValue).toBe("LALALAND");

        // Reset the value
        dataset.setFieldValue(MAKE, test_data[i].make);

        // Wait for database to be updated
        await sleep(1000);

        // Reload the dataset
        await dataset.load();

        // Find the row with id = i again
        dataset.navigator().moveToFind(new Map([[ID, i]]));

        // Find the new values
        newValue = dataset.getField(MAKE).value;

        // Ensure that the value has been changed back
        expect(newValue).toBe(test_data[i].make);

        await sleep(1000);
    }

    },20000);

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
