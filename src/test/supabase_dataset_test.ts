import {SupabaseDataPump} from "../sql_database_pump";
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
const COLUMN_NAMES_AS_STRING = MAKE + "," + MODEL+ "," + YEAR
const sleep = (ms) => new Promise(r => setTimeout(r, ms));


function createDataset() {

    //        .eq('id', 1).select();
    //}
    //.filter(this.sqlParameters?.filter);

    let supabaseDatasetPump = new SupabaseDataPump(credentials, true);

    supabaseDatasetPump.select = () => {
        return supabaseDatasetPump.supabaseClient.from(TABLE_NAME).select();
    }

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
test( "Update and Reset",  async () => {

    let dataset = createDataset();
    await dataset.load();

    console.log(dataset.json_d);

    // Find the row with id = 1
    let foundRows = dataset.navigator().find(new Map([["id",1]]));
    let theRow = foundRows.next().value;

    // Find the original values
    let originalValue = theRow.getField("make").value;
    let id = dataset.getField("id").value;

    // Ensure that the values are correct
    expect(originalValue).toBe("Chevrolet");
    expect(id).toBe(1);

    // Change the value
    dataset.setFieldValue("make","LALALAND");

    // Wait for database to be updated
    await sleep(1000);

    // Reload the dataset
    await dataset.load();
    console.log(dataset.json_d);

    // Find the row with id = 1 again
    foundRows = dataset.navigator().find(new Map([["id",1]]));

    theRow = foundRows.next().value;

    // Find the new values
    let newValue = theRow.getField("make").value;
    let newId = theRow.getField("id").value;

    // Ensure that the values are correct i.e. the update was successful
    expect(newId).toBe(id);
    expect(newValue).toBe("LALALAND");

    // Reset the value
    dataset.setFieldValue("make",originalValue);

    await sleep(1000);

    await dataset.load();
    console.log(dataset.json_d);

    await sleep(1000);

});

class Waiter {
    private timeout: any
    constructor() {
        this.waitLoop()
    }
    private waitLoop():void {
        this.timeout = setTimeout(() => { this.waitLoop() }, 100 * 1000)
    }
    okToQuit():void {
        clearTimeout(this.timeout)
    }
}
