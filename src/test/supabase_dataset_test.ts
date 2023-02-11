import {FieldType} from "../dataset";
import {SQLDatabasePump} from "../sql_database_pump";
import {KeyedPersistentDataset} from "../persistent_dataset";

const credentials = {
    url: 'https://pnjvoeaweebjdtzyhmqg.supabase.co',
    key: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBuanZvZWF3ZWViamR0enlobXFnIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NzI3MzA4MTMsImV4cCI6MTk4ODMwNjgxM30._MCnRXIgTE93HMAHILNtGftZGztBpuTgLBlGGum6zfs'
}

const TABLE_NAME = "persistence_test"
const MAKE = "make"
const MODEL = "model"
const YEAR = "year"
const ID = "id"
const COLUMN_NAMES_AS_STRING = MAKE + "," + MODEL+ "," + YEAR
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function createDataset() {
    let supabaseDatasetPump = new SQLDatabasePump(credentials, true);

    //supabaseDatasetPump.update = () => {
    //    return supabaseDatasetPump.supabaseClient.from(TABLE_NAME).update({ name: 'Australia' })
    //        .eq('id', 1).select();
    //}
    //.filter(this.sqlParameters?.filter);

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

test('Supabase test', async () => {
    let dataset = createDataset();

    await dataset.load();
    console.log(dataset.json_d);

});

test( "STUFF",  async () => {

    let dataset = createDataset();
    await dataset.load();

    console.log(dataset.json_d);

    dataset.navigator().first();

    let originalValue = dataset.getField("make").value;
    console.log("At " + originalValue);

    dataset.setFieldValue("make","LALALAND");

    await sleep(1000);

    let reloaded = createDataset();
    await reloaded.load();
    console.log(reloaded.json_d);

    reloaded.setFieldValue("make",originalValue);
    await sleep(1000);


    reloaded = createDataset();
    await reloaded.load();
    console.log(reloaded.json_d);

    await sleep(1000);

});