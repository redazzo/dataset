import {Dataset, FieldType} from "../dataset";
import {SupabaseDatasetPump} from "../supabase.dataset.pump";


test('Supabase test', async () => {

    let populator = new SupabaseDatasetPump(
        {
            url: '',
            key: ''
        }
        );

    populator.setSQLParameters({
        from: "persistence_test",
        select: "make, model, year"
    })

    const columnTypes = [
        {
            name: "make",
            type: FieldType.STRING
        },
        {
            name: "model",
            type: FieldType.STRING
        },
        {
            name: "year",
            type: FieldType.INTEGER
        }
    ];


    let dataset = new Dataset(columnTypes);
    await dataset.load(populator);
    console.log(dataset.json);




});