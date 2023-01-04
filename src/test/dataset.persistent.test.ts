import {FilePersistentDataPump, PersistentDataset} from "../persistent.dataset";
import {FieldType} from "../dataset";




test( 'Test file persistent dataset read', () => {

    let persistentDataPump = new FilePersistentDataPump("./test/persistent_test_data.json")

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

        persistentDataPump

    );

    persistentDataset.load();

    expect(persistentDataset.rowCount).toBe(4);

    let expectedNames = ["Bob", "Jane", "Mary", "David"];
    let expectedNamesPlus = expectedNames.slice();
    expectedNamesPlus.push("Bob NewSave");

    testEquality(persistentDataset, expectedNames);

    let newRow = persistentDataset.addRow();

    newRow.setFieldValue("name", "Bob NewSave");
    newRow.setFieldValue("age", "99");

    expect(persistentDataset.rowCount).toBe(5);

    persistentDataset.save();

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

        persistentDataPump

    );

    newPersistentDataset.load();


    testEquality(newPersistentDataset, expectedNamesPlus);
    expect(newPersistentDataset.rowCount).toBe(5);

    let lastRow = newPersistentDataset.navigator().last().value;

    newPersistentDataset.deleteRow(lastRow.id);
    expect(newPersistentDataset.rowCount).toBe(4);


    testEquality(newPersistentDataset, expectedNames);
    newPersistentDataset.save();




})

function testEquality(persistentDataset: PersistentDataset, expectedNames: string[]) {
    let index = 0;
    for (let row of persistentDataset.iterator()) {
        expect(row.getValue("name")).toBe(expectedNames[index++]);
    }
}