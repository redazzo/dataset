import {Subject, Subscription} from "rxjs";
import {v4 as uuidv4} from 'uuid';

export enum FieldType {
    STRING,
    INTEGER,
    FLOAT,
    DATE
}

export interface FieldDescriptor {

    get name(): string

    get type(): FieldType
}

export interface Field extends FieldDescriptor {

    get id(): string

    get value(): string

    subscribe(observer: (e: DatasetEvent<Field, Field>) => void): Subscription
}

export class DefaultFieldDescriptor implements FieldDescriptor {

    constructor(
        public readonly name: string,
        public readonly type: FieldType) {
    }

}

export type FieldDescriptors = {
    name: string,
    type: FieldType
}[];


export class DatasetEvent<SourceType, DetailType> {

    constructor(
        public readonly id: string,
        public readonly detail: DetailType,
        public readonly source: SourceType,
        public readonly eventType: DatasetEventType ) {
    }
}

export enum DatasetEventType {

    FIELD_VALUE_INSERTED,
    FIELD_VALUE_UPDATED,
    FIELD_VALUE_DELETED,
    FIELD_VALUE_VALIDATED,

    ROW_INSERTED,
    ROW_UPDATED,
    ROW_DELETED,
    ROW_VALIDATED,

    DATASET_UPDATED

}

export class TypedField implements Field {

    public readonly id: string = uuidv4();
    private readonly subject: Subject<DatasetEvent<Field, Field>> = new Subject<DatasetEvent<Field, Field>>();


    constructor(public readonly name: string, private fieldValue: string, public readonly type: FieldType) {
    }

    get value(): string {
        return this.fieldValue;
    }

    set value(value: string) {
        this.fieldValue = value;
        this.subject.next(new DatasetEvent<Field, Field>(this.id, this, this, DatasetEventType.FIELD_VALUE_UPDATED));
    }

    public subscribe(observer: (e: DatasetEvent<Field, Field>) => void): Subscription {
        return this.subject.subscribe(observer);
    }

}

export interface DataRow {

    getField(fieldName: string): Field

    getValue(fieldName: string): string

    setFieldValue(fieldName: string, value: string): void

    subscribe(observer: (v: DatasetEvent<DataRow, Field | DataRow>) => void): Subscription

}

export interface DataPump {

    load(dataset: Dataset): void
}

export interface PersistentDataPump extends DataPump {

    save(dataset: Dataset): void
}


export class DatasetRow implements DataRow {

    public readonly id: string;
    private readonly subject: Subject<DatasetEvent<DatasetRow, Field>> = new Subject<DatasetEvent<DatasetRow, Field>>();
    private fieldDescriptors: Map<string, FieldDescriptor> = new Map<string, FieldDescriptor>();
    private datasetFieldMap: Map<string, TypedField> = new Map<string, TypedField>();

    constructor(fieldDescriptors?: FieldDescriptor[]) {
        this.id = uuidv4();

        if (fieldDescriptors != null) {
            fieldDescriptors.forEach(value => {
                this.addFieldDescriptor(value);
                this.addField(value);
            });
        }
    }

    get typeHash(): number {

        return calculateTypeHash(this.fieldDescriptors);

    }

    public addColumn(fieldName: string, type: FieldType) {
        let fieldDescriptor = new DefaultFieldDescriptor(fieldName, type);
        this.addFieldDescriptor(fieldDescriptor);
        this.addField(fieldDescriptor);
    }

    public getField(fieldName: string): TypedField {
        return this.datasetFieldMap.get(fieldName);
    }

    public getValue(fieldName: string): string {
        return this.datasetFieldMap.get(fieldName).value;
    }

    public setFieldValue(fieldName: string, value: string): void {

        let tf: TypedField = this.datasetFieldMap.get(fieldName);

        if (tf == null || tf == undefined) {

            let fieldDescriptor = this.fieldDescriptors.get(fieldName);

            if (fieldDescriptor == null) {
                throw new Error("No such field.");
            } else {

                tf = this.addField(fieldDescriptor);
            }
        }

        tf.value = value;
    }

    public entries(): IterableIterator<TypedField> {
        return this.datasetFieldMap.values();
    }

    public subscribe(observer: (v: DatasetEvent<DatasetRow, Field>) => void): Subscription {
        return this.subject.subscribe(observer);
    }

    private addField(value: FieldDescriptor): TypedField {

        let thisRow = this;
        let tf = new TypedField(value.name, "", value.type);

        // Make the row an observer of the field, and fire an event if the field value changes.
        tf.subscribe((v: DatasetEvent<Field, Field>) => {

            thisRow.subject.next(
                new DatasetEvent<DatasetRow, Field>(thisRow.id, v.detail, thisRow, DatasetEventType.ROW_UPDATED)
            );
        });

        this.datasetFieldMap.set(value.name, tf);
        return tf;
    }

    private addFieldDescriptor(fieldDescription: FieldDescriptor): void {
        this.fieldDescriptors.set(fieldDescription.name, fieldDescription);
    }


}

class DatasetRowNavigator implements NavigableIterator<DatasetRow> {

    private iterationIndex: number;
    //private readonly maxSize: number;
    private theCurrentRow: DatasetRow;

    public constructor(private readonly datasetRows: Map<number, DatasetRow>) {
        //this.maxSize = datasetRows.size;
        this.reset();
    }

    public get index() {
        return this.iterationIndex;
    }

    public [Symbol.iterator](): IterableIterator<DatasetRow> {
        return this;
    }

    public reset() {
        this.iterationIndex = -1;
        if (this.datasetRows.size > 0) {
            this.theCurrentRow = this.datasetRows.get(0);
        } else {
            this.theCurrentRow = null;
        }
    }

    public first(): IteratorResult<DatasetRow> {
        if (this.datasetRows.size > 0) {
            this.iterationIndex = 0;
            this.theCurrentRow = this.datasetRows.get(this.iterationIndex);
        } else {
            this.theCurrentRow = null;
        }

        return {
            value: this.theCurrentRow
        }
    }

    public prior(): IteratorResult<DatasetRow> {
        if (this.iterationIndex > 0) {
            this.iterationIndex--;
            this.theCurrentRow = this.datasetRows.get(this.iterationIndex);

            return {
                value: this.theCurrentRow
            }

        }

        return {
            value: undefined,
            done: true
        }
    }

    public current(): IteratorResult<DatasetRow> {
        return {
            value: this.theCurrentRow
        }
    }

    public last() : IteratorResult<DatasetRow> {

        let maxIndex = findMaxIndex(this.datasetRows);

        this.theCurrentRow = this.datasetRows.get(maxIndex);

        return {
            value: this.theCurrentRow
        }
    }



    public next(...args: [] | [undefined]): IteratorResult<DatasetRow> {

        if (this.iterationIndex < this.datasetRows.size - 1) {

            this.iterationIndex++;
            this.theCurrentRow = this.datasetRows.get(this.iterationIndex);

            return {
                value: this.theCurrentRow
            }

        }

        return {
            value: undefined,
            done: true
        }
    }

    public moveTo(index : number) : IteratorResult<DatasetRow> {

        if (this.datasetRows.size <= 0){
            this.theCurrentRow = undefined;
            return {
                value: this.theCurrentRow,
                done: true
            }
        }

        if (index < 0) {
            this.reset();

            return {
                value: undefined,
                done: true
            }
        }

        this.iterationIndex = index;
        this.theCurrentRow = this.datasetRows.get(this.iterationIndex);

        let value = this.theCurrentRow;
        let done = this.iterationIndex == this.datasetRows.size - 1;

        return {
            value: value,
            done: done
        }
    }

    public getField(fieldName: string): Field {
        if (this.datasetRows.size == 0) return undefined;

        return this.theCurrentRow.getField(fieldName);
    }

    public getValue(fieldName: string): string {
        if (this.datasetRows.size == 0) return undefined;

        return this.theCurrentRow.getValue(fieldName);
    }

    public setFieldValue(fieldName: string, value: string): void {
        if (this.datasetRows.size == 0) throw new Error("Dataset has no rows.");
        return this.theCurrentRow.setFieldValue(fieldName, value);
    }

    subscribe(observer: (v: DatasetEvent<DataRow, Field>) => void): Subscription {
        return this.theCurrentRow.subscribe(observer);
    }

}

function findMaxIndex( m : Map<number, DatasetRow>) {
    //let maxIndex = 0;

    // TODO Consider creating a map that keeps track of the maximum index if performance becomes an issue.
    //for (let key of m.keys()) {
    //    if (key > maxIndex) maxIndex = key;
    //}
    //return maxIndex;

    return m.size - 1;
}

export interface NavigableIterator<T> extends IterableIterator<T>, DataRow {

    first(): IteratorResult<T>

    prior(): IteratorResult<T>

    current(): IteratorResult<T>

    last(): IteratorResult<T>

    reset(): void

    moveTo(index : number) : IteratorResult<T>

    readonly index

}

export class Dataset implements DataRow {

    private readonly subject: Subject<DatasetEvent<Dataset, DatasetRow>> = new Subject<DatasetEvent<Dataset, DatasetRow>>();
    private readonly theFieldDescriptors: Map<string, FieldDescriptor> = new Map<string, FieldDescriptor>();
    private theRows: Map<number, DatasetRow> = new Map<number, DatasetRow>();
    private readonly datasetId: string;
    private readonly typeHash: number;

    private theNavigator: DatasetRowNavigator = null;

    constructor(fieldDescriptors: FieldDescriptors);

    constructor(fieldDescriptors: FieldDescriptors, rows?: DatasetRow[]) {
        this.datasetId = uuidv4();

        this.theFieldDescriptors = new Map<string, FieldDescriptor>();
        for (let descriptor of fieldDescriptors) {

            this.theFieldDescriptors.set(descriptor.name, descriptor);
        }

        this.typeHash = calculateTypeHash(this.theFieldDescriptors);

        if (rows != null) {

            let noRows = 0;
            for (let row of rows) {

                if (row.typeHash != this.typeHash) {
                    throw new Error("Rows in a dataset must have identical fields and field types.");
                }

                this.theRows.set(noRows++, row);
            }
        }
    }

    /**
     * Returns a clone of the field descriptors
     */
    get fieldDescriptors(): Map<string, FieldDescriptor> {

        let descriptors = new Map<string, FieldDescriptor>();

        for (let aDescriptor of this.theFieldDescriptors.values()) {

            descriptors.set(aDescriptor.name, aDescriptor);
        }

        return descriptors;
    }

    get id(): string {
        return this.datasetId;
    }

    get rowCount(): number {
        return this.theRows.size;
    }

    public get json(): {} {

        let dso = {};
        for (let row of this.iterator()) {

            let ro = {}
            for (let field of row.entries()) {
                ro[field.name] = field.value;
            }

            dso[row.id] = ro;
        }

        return dso;

    }

    public get json_d(): {} {

        let data = [];
        for (let row of this.iterator()) {

            let ro = {}
            for (let field of row.entries()) {
                ro[field.name] = field.value;
            }

            data.push(ro);
        }

        return data;

    }

    public iterator(): IterableIterator<DatasetRow> {
        return new DatasetRowNavigator(this.theRows);
    }

    public navigator(): NavigableIterator<DatasetRow> {

        if (this.theNavigator == null) {
            this.theNavigator = new DatasetRowNavigator(this.theRows);
        }
        return this.theNavigator;
    }

    public addColumn(fieldName: string, type: FieldType) {

        let fd = this.theFieldDescriptors.get(fieldName);

        if (!(fd == null || fd == undefined)) {
            throw new Error("A column with that field name already exists!");
        }

        fd = new DefaultFieldDescriptor(fieldName, type);
        this.theFieldDescriptors.set(fieldName, fd);

    }

    public getField(fieldName: string): Field {
        return this.navigator().getField(fieldName);

    }

    public getValue(fieldName: string): string {
        return this.navigator().getValue(fieldName);

    }

    public setFieldValue(fieldName: string, value: string): void {
        this.navigator().setFieldValue(fieldName, value);
    }

    public getRow(rowId: string | number): DatasetRow {

        if (this.theRows.size == 0) return null;

        if (typeof rowId == "number"){
            return this.theRows.get(rowId);
        }

        for (let row of this.theRows.values()) {

            if (row.id === rowId) {
                return row;
            }

        }
        return null;
    }

    public getRowIds(): string[] {

        let rowIds = new Array<string>();

        for (let row of this.theRows.values()) {
            rowIds.push(row.id);
        }

        return rowIds;
    }

    public addRow(row?: DatasetRow): DatasetRow {

        let datasetThis = this;

        if (row == null || row == undefined) {

            let values = this.theFieldDescriptors.values();

            let fieldDescriptorsTmp = new Array<DefaultFieldDescriptor>();
            for (let value of values) {

                fieldDescriptorsTmp.push(value);

            }
            row = new DatasetRow(fieldDescriptorsTmp);

        }

        // Make the dataset an observer of the row, and fire an event if there is a change to the row
        row.subscribe((v) => {

            datasetThis.subject.next(new DatasetEvent<Dataset, DatasetRow>(datasetThis.datasetId, row, datasetThis,DatasetEventType.ROW_UPDATED));
        });

        let noRows = this.theRows.size
        this.theRows.set(noRows, row);
        this.navigator().last();
        this.subject.next(new DatasetEvent<Dataset, DatasetRow>(this.datasetId, row, this, DatasetEventType.ROW_INSERTED));
        return row;
    }

    public deleteRow(rowId : string | number) : DatasetRow {
        /*
        Deletion is an expensive operation as the order of the dataset is managed via a map of integers to rows. Removal
        of a row subsequently needs the map to be partially rebuilt.
         */

        let maxIndex = this.theRows.size - 1;

        let theRow = null;
        let key = -1;
        if (typeof rowId === "number" ){

            theRow = this.theRows.get(rowId);
            key = rowId;

        } else {

            for (let aKey of this.theRows.keys()) {

                let aRow = this.theRows.get(aKey);

                if (aRow.id === rowId) {
                    theRow = aRow;
                    key = aKey;
                }
            }
        }


        if (theRow == undefined || theRow == null){
            return null;
        }

        let lastRowDeleted = key == this.theRows.size - 1;
        this.theRows.delete(key);


        // Remap the remainder
        if (!lastRowDeleted){
            for (let i=key; i < maxIndex; i++){
                let nextRow = this.theRows.get(i + 1);
                this.theRows.set(i, nextRow);
            }
            this.theRows.delete(this.theRows.size - 1);

        }
        this.navigator().moveTo(key - 1);

        // TODO update DatasetEvent to carry a status of the change, e.g. "ADDED_ROW", "DELETED_ROW"
        this.subject.next(new DatasetEvent<Dataset, DatasetRow>(this.datasetId, theRow, this, DatasetEventType.ROW_DELETED));
    }

    public subscribe(observer: (v: DatasetEvent<Dataset, DataRow>) => void): Subscription {
        return this.subject.subscribe(observer);
    }

    public async load(pop: DataPump): Promise<void> {

        return pop.load(this);
    }

}


/**
 * Returns a hash from the field descriptors. A bit crap - but good enough.
 */
function calculateTypeHash(descriptors: Map<string, FieldDescriptor>): number {

    let hash = 0;

    for (let descriptor of descriptors.values()) {
        let name = descriptor.name + descriptor.type.toString();
        for (let i = 0; i < name.length; i++) {

            let chr;

            if (name.length === 0) return hash;

            for (let i = 0; i < name.length; i++) {

                chr = name.charCodeAt(i);
                hash = ((hash << 5) - hash) + chr;
                hash |= 0; // Convert to 32bit integer
            }

        }
    }

    return hash;

}

export class ObjectArrayDataPump implements DataPump {


    constructor(protected data : {}[]) {
    }

    public load(dataset: Dataset): void {
        for (let rowValues of this.data) {

            let row = new DatasetRow();
            let fieldDescriptors = dataset.fieldDescriptors;
            for (let entry of fieldDescriptors.values()) {

                row.addColumn(entry.name, entry.type);
            }

            // Seems a bit dirty
            let mp: Map<string, string> = new Map<string, string>(Object.entries(rowValues));

            for (let key of mp.keys()) {
                row.setFieldValue(key, mp.get(key));
            }

            dataset.addRow(row);

        }
    }
}

