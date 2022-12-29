import {
    Dataset,
    DatasetEvent,
    DatasetRow,
    DefaultFieldDescriptor,
    Field,
    FieldDescriptor,
    FieldType,
    defaultPopulator,
    TypedField
} from '../dataset'

const EMPTY_STRING = "";
const BOB = "BOB";
const MARY = "MARY";

const FIELD_NAME_1 = "FieldName1";


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

    const field = new TypedField(FIELD_NAME_1, EMPTY_STRING, FieldType.STRING);

    expect(field.name).toBe(FIELD_NAME_1);
    expect(field.value).toBe(EMPTY_STRING);

    field.value = BOB;
    expect(field.value).toBe(BOB);

    field.value = MARY;
    expect(field.value).toBe(MARY);

});

test('Test subscribed field observers', () => {

    const field = new TypedField(FIELD_NAME_1, EMPTY_STRING, FieldType.STRING);

    const testObserver1 = new Observer<Field, Field>();
    const testObserver2 = new Observer<Field, Field>();


    field.subscribe(testObserver1.observer);
    const subscription2 = field.subscribe(testObserver2.observer);

    field.value = BOB;

    //console.log("xxx:" + testObserver1.fieldName)

    expect(testObserver1.source.name).toBe(FIELD_NAME_1);
    expect(testObserver2.source.name).toBe(FIELD_NAME_1);
    expect(testObserver1.source.value).toBe(BOB);
    expect(testObserver2.source.value).toBe(BOB);

    subscription2.unsubscribe();
    field.value = MARY;

    expect(testObserver1.source.name).toBe(FIELD_NAME_1);
    expect(testObserver2.source.name).toBe(FIELD_NAME_1);
    expect(testObserver1.count).toBe(2);
    expect(testObserver2.count).toBe(1);

})


test('Test DatasetRow with Field Descriptor array', () => {

    const fieldTypes = {
        "name": FieldType.STRING,
        "age": FieldType.INTEGER
    }


    const nameFieldD: FieldDescriptor = new DefaultFieldDescriptor("name", FieldType.STRING);
    const ageFieldD: FieldDescriptor = new DefaultFieldDescriptor("age", FieldType.INTEGER);


    const row: DatasetRow = new DatasetRow([nameFieldD, ageFieldD]);

    doDatarowTest(row);

});

test('Test DatasetRow', () => {

    const row: DatasetRow = new DatasetRow();

    row.addColumn("name", FieldType.STRING);
    row.addColumn("age", FieldType.INTEGER);

    doDatarowTest(row);

});

test('Test type hash of datasetrow', () => {

    const row1 = new DatasetRow();
    const row2 = new DatasetRow();
    const row3 = new DatasetRow();

    row1.addColumn("name", FieldType.STRING);
    row1.addColumn("age", FieldType.INTEGER);
    row1.addColumn("eyecolour", FieldType.STRING);

    row2.addColumn("name", FieldType.STRING);
    row2.addColumn("age", FieldType.INTEGER);
    row2.addColumn("eyecolour", FieldType.STRING);

    row3.addColumn("name", FieldType.STRING);
    row3.addColumn("age", FieldType.INTEGER);
    //row3.addColumn("eyec_colour", FieldType.STRING);

    expect(row1.typeHash == row2.typeHash).toBe(true);
    expect(row1.typeHash == row3.typeHash).toBe(false);

});

test('Dataset Test', () => {

    const theObserver = new Observer<Dataset, DatasetRow>();

    const columnTypes = [
        {
            name: "name",
            type: FieldType.STRING
        },
        {
            name: "age",
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

    const theObserver = new Observer<Dataset, DatasetRow>();

    const columnTypes = [
        { name: "name", type: FieldType.STRING },
        { name: "age",  type: FieldType.INTEGER }
    ];

    const dataSet: Dataset = new Dataset(columnTypes);
    dataSet.subscribe(theObserver.observer);

    expect(theObserver.count).toBe(0);

    let p = [
        { name: "Gerry", age: "51" },
        { name: "Deborah", age: "50"},
        { name: "Joshua", age: "12"},
        { name: "Helen", age: "75" }
    ]

    dataSet.populate(defaultPopulator(p));

    expect(dataSet.rowCount).toBe(4);
    expect(theObserver.count).toBe(4);

    const firstRow = dataSet.getRow(dataSet.getRowIds()[0]);
    expect(firstRow != null && firstRow != undefined).toBe(true);

    const field = firstRow.getField("age");
    expect(field != null && field != undefined).toBe(true);

    expect(firstRow.getValue("age")).toBe("51");

    firstRow.setFieldValue("age", "52");
    expect(firstRow.getValue("age")).toBe("52");
    expect(theObserver.count).toBe(5);

    console.log("TESTING ITERATOR 8888888888888888888888888888888888");
    let index = 0;
    for (let itr of dataSet.iterator()){
        expect(itr.getValue("name")).toBe(p[index++].name);
    }

    index = 0;
    let navigator = dataSet.navigator();
    for (let itr of navigator){
        expect(itr.getValue("name")).toBe(p[index++].name);
    }




    let aRow = navigator.first().value;

    expect(aRow.getValue("name")).toBe("Gerry");
    aRow = navigator.current().value;
    expect(aRow.getValue("name")).toBe("Gerry");

    aRow = navigator.next().value;
    expect(aRow.getValue("name")).toBe("Deborah");


})


function doDatarowTest(row: DatasetRow) {
    const theObserver = new Observer<DatasetRow, Field>();

    row.subscribe(theObserver.observer);

    row.setFieldValue("name", "Gerry");
    expect(theObserver.detail.name).toBe("name");
    expect(theObserver.detail.value).toBe("Gerry");

    row.setFieldValue("age", "51");
    expect(theObserver.detail.name).toBe("age");
    expect(theObserver.detail.value).toBe("51");

    row.setFieldValue("age", "52");
    expect(theObserver.detail.value).toBe("52");

    expect(row.getValue("age")).toBe("52");

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
                expect(field.name).toBe("name");
                break;
            case 1 :
                expect(field.name).toBe("age");
                break;
            default:
                break;
        }

        count++;

    }
}







