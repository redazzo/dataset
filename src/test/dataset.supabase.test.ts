import {Dataset, FieldType} from "../dataset";
import {SupabaseDatasetPump} from "../supabase.dataset.pump";

const credentials = {
    url: 'https://pnjvoeaweebjdtzyhmqg.supabase.co',
    key: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBuanZvZWF3ZWViamR0enlobXFnIiwicm9sZSI6ImFub24iLCJpYXQiOjE2NzI3MzA4MTMsImV4cCI6MTk4ODMwNjgxM30._MCnRXIgTE93HMAHILNtGftZGztBpuTgLBlGGum6zfs'
}

const TABLE_NAME = "persistence_test"
const MAKE = "make"
const MODEL = "model"
const YEAR = "year"
const COLUMN_NAMES_ARRAY = [MAKE, MODEL, YEAR]
const COLUMN_NAMES_AS_STRING = MAKE + "," + MODEL+ "," + YEAR

test('Supabase test', async () => {

    let populator = new SupabaseDatasetPump(credentials);

    populator.setSQLParameters({
        from: TABLE_NAME,
        select: COLUMN_NAMES_AS_STRING
    })

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
    await dataset.load(populator);
    console.log(dataset.json);




});