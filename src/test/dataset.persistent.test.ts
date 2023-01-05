import {FilePersistentDataPump, PersistentDataset} from "../persistent.dataset";
import {FieldType} from "../dataset";

let expectedNames = ["Bob", "Jane", "Mary", "David"];

test( 'Test file persistent dataset read, add row, save, reload from disk with new row, delete row, and save back to original state', async () => {

    let persistentDataPump_1 = new FilePersistentDataPump("./test/persistent_test_data.json")

    let persistentDataset = new PersistentDataset(
        [
            {
                name: "name",
                type: FieldType.STRING
            },
            {
                name: "age",
                type: FieldType.INTEGER
            }],

        persistentDataPump_1

    );

    persistentDataset.load();

    // TEST *******************************************
    expect(persistentDataset.rowCount).toBe(4);




    // TEST *******************************************
    testEquality(persistentDataset, expectedNames);

    let newRow = persistentDataset.addRow();

    newRow.setFieldValue("name", "Bob NewSave");
    newRow.setFieldValue("age", "99");

    // TEST *******************************************
    expect(persistentDataset.rowCount).toBe(5);

    persistentDataset.save();



    let expectedNamesPlus = expectedNames.slice();
    expectedNamesPlus.push("Bob NewSave");

    let persistentDataPump_2 = new FilePersistentDataPump("./test/persistent_test_data.json")

    let newPersistentDataset = new PersistentDataset(
        [
            {
                name: "name",
                type: FieldType.STRING
            },
            {
                name: "age",
                type: FieldType.INTEGER
            }],

        persistentDataPump_2

    );

    newPersistentDataset.load();


    // TEST *******************************************
    testEquality(newPersistentDataset, expectedNamesPlus);

    // TEST *******************************************
    expect(newPersistentDataset.rowCount).toBe(5);

    let lastRow = newPersistentDataset.navigator().last().value;

    newPersistentDataset.deleteRow(lastRow.id);

    // TEST *******************************************
    expect(newPersistentDataset.rowCount).toBe(4);

    // TEST *******************************************
    testEquality(newPersistentDataset, expectedNames);
    newPersistentDataset.save();


})


function testEquality(persistentDataset: PersistentDataset, expectedNames: string[]) {
    let index = 0;
    for (let row of persistentDataset.iterator()) {
        expect(row.getValue("name")).toBe(expectedNames[index++]);
    }
}