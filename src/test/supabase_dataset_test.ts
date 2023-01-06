import {Dataset, FieldType} from "../dataset";
import {SQLDatabasePump} from "../sql_database_pump";

const credentials = {
    url: 'https://pnjvoeaweebjdtzyhmqg.supabase.co',
    key: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBuanZvZWF3ZWViamR0enlobXFnIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NzI3MzA4MTMsImV4cCI6MTk4ODMwNjgxM30._MCnRXIgTE93HMAHILNtGftZGztBpuTgLBlGGum6zfs'
}

const TABLE_NAME = "persistence_test"
const MAKE = "make"
const MODEL = "model"
const YEAR = "year"
//const COLUMN_NAMES_AS_STRING = MAKE + "," + MODEL+ "," + YEAR

test('Supabase test', async () => {

    let supabaseDatasetPump = new SQLDatabasePump(credentials);

    supabaseDatasetPump.select = () => {
        return supabaseDatasetPump.supabaseClient.from('persistence_test').select('make, model, year', {count: 'exact'}).eq('id','2');
    }
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
        }
    ];

    let dataset = new Dataset(columnTypes);
    await dataset.load(supabaseDatasetPump);
    console.log(dataset.json_d);

});