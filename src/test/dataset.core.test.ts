import {
    Dataset,
    DatasetEvent,
    DatasetRow,
    DefaultFieldDescriptor,
    Field,
    FieldDescriptor,
    FieldType,
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



class Observer<SourceType, DetailType> {

    public detail: DetailType;
    public id: string;
    public source: SourceType;
    public count = 0;

    private theObserver: (v: DatasetEvent<SourceType, DetailType>) => void = (v: DatasetEvent<SourceType, DetailType>) => {

        this.id = v.id;
        this.detail = v.detail;
        this.source = v.source;
        this.count = this.count + 1;
    }

    public get observer(): (v: DatasetEvent<SourceType, DetailType>) => void {
        return this.theObserver;
    }
}

test('Dataset value is updated', () => {

    const field = new TypedField(FIELDNAME1, EMPTY_STRING, FieldType.STRING);

    expect(field.name).toBe(FIELDNAME1);
    expect(field.value).toBe(EMPTY_STRING);

    field.value = BOB;
    expect(field.value).toBe(BOB);

    field.value = MARY;
    expect(field.value).toBe(MARY);

});

test('Test subscribed field observers', () => {

    const field = new TypedField(FIELDNAME1, EMPTY_STRING, FieldType.STRING);

    const testObserver1 = new Observer<Field, Field>();
    const testObserver2 = new Observer<Field, Field>();


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

test('Dataset Test', () => {

    const theObserver = new Observer<Dataset, DatasetRow>();

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



test('Populate dataset test', () => {

    let {theObserver, dataSet} = populateDataset();

    expect(dataSet.rowCount).toBe(4);
    expect(theObserver.count).toBe(4);

    const firstRow = dataSet.getRow(dataSet.getRowIds()[0]);
    expect(firstRow != null && firstRow != undefined).toBe(true);

    const field = firstRow.getField(FIELDNAME2);
    expect(field != null && field != undefined).toBe(true);

    expect(firstRow.getValue(FIELDNAME2)).toBe(AGE1);

    firstRow.setFieldValue(FIELDNAME2, "52");
    expect(firstRow.getValue(FIELDNAME2)).toBe("52");
    expect(theObserver.count).toBe(5);


})

test('Test iterator and navigator', () => {

    let {dataSet, p} = populateDataset();

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

test('Delete row test', () => {

    let {dataSet, p} = populateDataset();

    let firstRow = dataSet.navigator().first().value;

    dataSet.navigator().reset();

    for (let row of dataSet.navigator()){
        console.log("index: " + dataSet.navigator().index + "         name: " + row.getValue(FIELDNAME1));
    }

    console.log("DELETING _______________________________00");

    dataSet.deleteRow(2);

    dataSet.navigator().reset();

    for (let row of dataSet.navigator()){
        console.log("index: " + dataSet.navigator().index + "            id: " + row.id + "         name: " + row.getValue(FIELDNAME1));
    }

    let aRow = dataSet.getRow(2);
    let aRowId = aRow.id;

    console.log("About to remove " + aRowId);

    dataSet.deleteRow(aRowId);

    for (let row of dataSet.iterator()){
        console.log("index: " + row.id + "         name: " + row.getValue(FIELDNAME1));
    }


})

function populateDataset() {
    const theObserver = new Observer<Dataset, DatasetRow>();

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

    dataSet.load(new ObjectArrayDataPump(p));
    return {theObserver, dataSet, p};
}


function doDatarowTest(row: DatasetRow) {
    const theObserver = new Observer<DatasetRow, Field>();

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







