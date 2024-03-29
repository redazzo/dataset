import {
    Dataset,
    DatasetRow,
    DefaultFieldDescriptor,
    DefaultObserver, Field, FieldDescriptor, FieldType,
    ObjectArrayDataPump,
    TypedField
} from '../dataset'

const EMPTY_STRING = "";
const BOB = "BOB";
const MARY = "MARY";

const NAME1 = BOB;
const NAME2 = "Jerry";
const NAME3 = "Jane";
const NAME4 = "Tom";

const AGE1 = "51";
const AGE2 = "50";
const AGE3 = "12";
const AGE4 = "75";

const FIELDNAME1 ="name";
const FIELDNAME2 = "age";


// Test that a field is created with the correct name and value
test('Test updating of TypedField', () => {

    const field = new TypedField(FIELDNAME1, EMPTY_STRING, FieldType.STRING);

    expect(field.name).toBe(FIELDNAME1);
    expect(field.value).toBe(EMPTY_STRING);

    field.value = BOB;
    expect(field.value).toBe(BOB);

    field.value = MARY;
    expect(field.value).toBe(MARY);

});

// Test that the field can be updated and that the observers are notified
test('Test subscribed field observers', () => {

    const field = new TypedField(FIELDNAME1, EMPTY_STRING, FieldType.STRING);

    const testObserver1 = new DefaultObserver<Field, Field>();
    const testObserver2 = new DefaultObserver<Field, Field>();


    field.subscribe(testObserver1.observer);
    const subscription2 = field.subscribe(testObserver2.observer);

    field.value = BOB;

    expect(testObserver1.source.name).toBe(FIELDNAME1);
    expect(testObserver2.source.name).toBe(FIELDNAME1);
    expect(testObserver1.source.value).toBe(BOB);
    expect(testObserver2.source.value).toBe(BOB);

    subscription2.unsubscribe();
    field.value = MARY;

    expect(testObserver1.source.name).toBe(FIELDNAME1);
    expect(testObserver2.source.name).toBe(FIELDNAME1);
    expect(testObserver1.count).toBe(2);
    expect(testObserver2.count).toBe(1);

})


test('Test DatasetRow with Field Descriptor array', () => {

    const fieldTypes = {
        FIELDNAME1: FieldType.STRING,
        FIELDNAME2: FieldType.INTEGER
    }


    const nameFieldD: FieldDescriptor = new DefaultFieldDescriptor(FIELDNAME1, FieldType.STRING);
    const ageFieldD: FieldDescriptor = new DefaultFieldDescriptor(FIELDNAME2, FieldType.INTEGER);


    const row: DatasetRow = new DatasetRow([nameFieldD, ageFieldD]);

    doDatarowTest(row);

});

test('Test DatasetRow', () => {

    const row: DatasetRow = new DatasetRow();

    row.addColumn(FIELDNAME1, FieldType.STRING);
    row.addColumn(FIELDNAME2, FieldType.INTEGER);

    doDatarowTest(row);

});

// Test that the hash of a row is the same for two rows with the same fields
test('Test type hash of datasetrow', () => {

    const row1 = new DatasetRow();
    const row2 = new DatasetRow();
    const row3 = new DatasetRow();

    row1.addColumn(FIELDNAME1, FieldType.STRING);
    row1.addColumn(FIELDNAME2, FieldType.INTEGER);
    row1.addColumn("eyecolour", FieldType.STRING);

    row2.addColumn(FIELDNAME1, FieldType.STRING);
    row2.addColumn(FIELDNAME2, FieldType.INTEGER);
    row2.addColumn("eyecolour", FieldType.STRING);

    row3.addColumn(FIELDNAME1, FieldType.STRING);
    row3.addColumn(FIELDNAME2, FieldType.INTEGER);
    //row3.addColumn("eyec_colour", FieldType.STRING);

    expect(row1.typeHash == row2.typeHash).toBe(true);
    expect(row1.typeHash == row3.typeHash).toBe(false);

});

// Test that a dataset is created correctly, and when adding rows, that the row count is correct
test('Dataset Test', () => {

    const theObserver = new DefaultObserver<Dataset, DatasetRow>();

    const columnTypes = [
        {
            name: FIELDNAME1,
            type: FieldType.STRING
        },
        {
            name: FIELDNAME2,
            type: FieldType.INTEGER
        }
    ];

    const dataSet: Dataset = new Dataset(columnTypes);

    dataSet.subscribe(theObserver.observer);

    dataSet.addRow();
    expect(dataSet.rowCount).toBe(1);
    expect(theObserver.source).toBe(dataSet);

    dataSet.addRow();
    expect(dataSet.rowCount).toBe(2);

    dataSet.addRow();
    expect(dataSet.rowCount).toBe(3);

    const rowIds = dataSet.getRowIds();

    expect(rowIds.length).toBe(3);

    let tempRow: DatasetRow = null;
    let row: DatasetRow = null;
    for (let rowId of rowIds) {
        row = dataSet.getRow(rowId);
        expect(row != null || row != undefined).toBe(true);
        expect(row != tempRow).toBe(true);
        tempRow = row;
    }

    expect(dataSet.rowCount).toBe(3);

});


// Test that row observers are notified when a row is updated
test('Populate dataset test', async () => {

    let {theObserver, dataSet} = await populateDataset();

    expect(dataSet.rowCount).toBe(4);
    expect(theObserver.count).toBe(0);

    const firstRow = dataSet.getRow(dataSet.getRowIds()[0]);
    expect(firstRow != null && firstRow != undefined).toBe(true);

    const field = firstRow.getField(FIELDNAME2);
    expect(field != null && field != undefined).toBe(true);

    expect(firstRow.getValue(FIELDNAME2)).toBe(AGE1);

    const rowObserver = new DefaultObserver<DatasetRow, Field>();
    firstRow.subscribe(rowObserver.observer);

    expect(rowObserver.count).toBe(0);
    firstRow.setFieldValue(FIELDNAME2, "52");

    expect(firstRow.getValue(FIELDNAME2)).toBe("52");
    expect(rowObserver.count).toBe(1);


})

// Test that the dataset iterator works
test('Test iterator and navigator', async () => {

    let {dataSet, p} = await populateDataset();

    let index = 0;
    for (let itr of dataSet.iterator()){
        expect(itr.getValue(FIELDNAME1)).toBe(p[index++].name);
    }

    index = 0;
    let navigator = dataSet.navigator();
    for (let itr of navigator){
        expect(itr.getValue(FIELDNAME1)).toBe(p[index++].name);
    }

    let aRow = navigator.first().value;

    expect(aRow.getValue(FIELDNAME1)).toBe(NAME1);
    aRow = navigator.current().value;
    expect(aRow.getValue(FIELDNAME1)).toBe(NAME1);

    aRow = navigator.next().value;

    expect(aRow.getValue(FIELDNAME1)).toBe(NAME2);

    aRow = navigator.prior().value;
    expect(aRow.getValue(FIELDNAME1)).toBe(NAME1);

    let result = navigator.prior();
    expect(result.done).toBe(true);
    expect(result.value).toBe(undefined);

    result = navigator.prior();
    expect(result.done).toBe(true);
    expect(result.value).toBe(undefined);

    result = navigator.first();
    expect(aRow.getValue(FIELDNAME1)).toBe(NAME1);
    aRow = navigator.current().value;
    expect(aRow.getValue(FIELDNAME1)).toBe(NAME1);


    index = 1;
    for (let itr of navigator){
        expect(itr.getValue(FIELDNAME1)).toBe(p[index++].name);
    }


})

// Test that row deletion works
test('Delete row test', async () => {

    let {dataSet, p} = await populateDataset();

    expect(dataSet.rowCount).toBe(4);

    dataSet.deleteRow(2);

    expect(dataSet.rowCount).toBe(3);

    let expectedNames = [NAME1, NAME2, NAME4];
    let expectedNamesIterator = expectedNames.values();
    for (let row of dataSet.iterator()){
        expect(row.getValue(FIELDNAME1)).toBe(expectedNamesIterator.next().value);
    }

    let aRow = dataSet.getRow(2);
    let aRowId = aRow.id;

    dataSet.deleteRow(aRowId);
    expect(dataSet.rowCount).toBe(2);

    expectedNames = [NAME1, NAME2];

    let expectedIterator = expectedNames.values();
    for (let row of dataSet.iterator()){
        let expected = expectedIterator.next().value;
        expect(row.getValue(FIELDNAME1)).toBe(expected);
    }


    dataSet.deleteRow(1); // This will move the current row to be the 0th row, as a delete moves the cursor to the prior row.
    expect(dataSet.rowCount).toBe(1);
    expect(dataSet.getValue(FIELDNAME1)).toBe(NAME1);

    dataSet.deleteRow(0);
    expect(dataSet.rowCount).toBe(0);
    expect(dataSet.getValue(FIELDNAME1)).toBe(undefined);



})

// Populate a dataset with test data
async function populateDataset() : Promise<{theObserver: DefaultObserver<Dataset, DatasetRow>, dataSet: Dataset, p: any[]}> {
    const theObserver = new DefaultObserver<Dataset, DatasetRow>();

    const columnTypes = [
        {name: FIELDNAME1, type: FieldType.STRING},
        {name: FIELDNAME2, type: FieldType.INTEGER}
    ];

    const dataSet: Dataset = new Dataset(columnTypes);
    dataSet.subscribe(theObserver.observer);

    expect(theObserver.count).toBe(0);

    let p = [
        {name: NAME1, age: AGE1},
        {name: NAME2, age: AGE2},
        {name: NAME3, age: AGE3},
        {name: NAME4, age: AGE4}
    ]

    await dataSet.load(new ObjectArrayDataPump(p));
    return {theObserver, dataSet, p};
}

// Various tests for the dataset row
function doDatarowTest(row: DatasetRow) {
    const theObserver = new DefaultObserver<DatasetRow, Field>();

    row.subscribe(theObserver.observer);

    row.setFieldValue(FIELDNAME1, NAME1);
    expect(theObserver.detail.name).toBe(FIELDNAME1);
    expect(theObserver.detail.value).toBe(NAME1);

    row.setFieldValue(FIELDNAME2, AGE1);
    expect(theObserver.detail.name).toBe(FIELDNAME2);
    expect(theObserver.detail.value).toBe(AGE1);

    row.setFieldValue(FIELDNAME2, "52");
    expect(theObserver.detail.value).toBe("52");

    expect(row.getValue(FIELDNAME2)).toBe("52");

    let errorThrown = false;
    try {
        row.setFieldValue("error", "error");
    } catch (e) {
        errorThrown = true;
    }

    expect(errorThrown).toBe(true);


    let count = 0;

    const iterator: IterableIterator<TypedField> = row.entries();
    for (let field of iterator) {

        switch (count) {

            case 0 :
                expect(field.name).toBe(FIELDNAME1);
                break;
            case 1 :
                expect(field.name).toBe(FIELDNAME2);
                break;
            default:
                break;
        }

        count++;

    }
}







