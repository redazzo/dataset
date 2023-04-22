import {Subject, Subscription} from "rxjs";
import {v4 as uuidv4} from 'uuid';
import {PersistentDataset} from "./persistent_dataset";

export enum FieldType {
    STRING,
    INTEGER,
    FLOAT,
    DATE,
    BOOLEAN // Todo - add more types
}

/**
 * A FieldDescriptor is a description of a field in a dataset.
 */
export interface FieldDescriptor {

    get name(): string

    get type(): FieldType
}

/**
 * A Field is a value in a DataRow.
 */
export interface Field extends FieldDescriptor {

    get id(): string

    get value(): string

    subscribe(observer: (e: DatasetEvent<Field, Field>) => void): Subscription

    get modified(): boolean

    resetModified(): void
}

/**
 * FieldDescriptors are a list of FieldDescriptors.
 */
export type FieldDescriptors = {
    name: string,
    type: FieldType
}[];

/**
 * A DatasetEventType is an event that can occur on a dataset.
 */
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

/**
 * A DataRow is a row in a dataset.
 */
export interface DataRow {

    getField(fieldName: string): Field

    getValue(fieldName: string): string

    setFieldValue(fieldName: string, value: string): void

    isRowPopulated(): boolean

    get modified(): boolean

    resetModified(): void

    subscribe(observer: (v: DatasetEvent<DataRow, Field | DataRow>) => void): Subscription

}

// Todo - turn this into a status class with a status, message, and optional error
export enum LoadStatus {
    SUCCESS,
    FAILURE
}

/**
 * A DataPump loads data into a dataset.
 */
export interface DataPump<T extends Dataset> {

    load(dataset: T): Promise<LoadStatus>
}

/**
 * A PersistentDataPump loads data into a dataset and saves data from a dataset.
 */
export interface PersistentDataPump<T extends PersistentDataset> extends DataPump<PersistentDataset> {

    save(dataset: T): Promise<void>

}

/**
 * A NavigableIterator is an iterator that can also navigate across a dataset.
 */
export interface NavigableIterator<T> extends IterableIterator<T>, DataRow {

    /**
     * Returns the index of the current row.
     */
    readonly index

    /**
     * Navigates to the first row.
     */
    first(): IteratorResult<T>

    /**
     * Navigates back to a prior row.
     */
    prior(): IteratorResult<T>

    /**
     * Navigates to the next row.
     */
    current(): IteratorResult<T>

    /**
     * Navigates to the last row.
     */
    last(): IteratorResult<T>

    /**
     * Resets the navigator to zeroth row.
     */
    reset(): void

    /**
     * Navigates directly to the entry at index.
     * @param index
     */
    moveTo(index: number): IteratorResult<T>

    /**
     * Navigate to the entry identified by the key value.
     * @param keys
     */
    moveToFind(keys: Map<string, number>): IterableIterator<T>

}

/**
 * A DatasetEvent is an event that occurs on a dataset.
 */
export class DatasetEvent<SourceType, DetailType> {

    constructor(
        public readonly id: string,
        public readonly detail: DetailType,
        public readonly source: SourceType,
        public readonly eventType: DatasetEventType) {
    }
}

/**
 * The DatasetRowNavigator class is an implementation of the NavigableIterator interface. It is used to navigate across the rows of a dataset.
 */
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

    public get modified(): boolean {
        if (this.datasetRows.size == 0) throw new Error("Dataset has no rows.");
        if (this.theCurrentRow == null) throw new Error("Current row is null.");

        return this.theCurrentRow.modified;
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

    public last(): IteratorResult<DatasetRow> {

        let maxIndex = this.datasetRows.size - 1;

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

    public moveTo(index: number): IteratorResult<DatasetRow> {

        if (this.datasetRows.size <= 0) {
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

    public moveToFind(keys: Map<string, number>): IterableIterator<DatasetRow> {


        if (this.datasetRows.size <= 0) {
            this.theCurrentRow = undefined;
            return {
                next(): IteratorResult<DatasetRow> {
                    return {
                        value: undefined,
                        done: true
                    }
                },

                [Symbol.iterator](): IterableIterator<DatasetRow> {
                    return this;
                }
            }
        }

        const rows: DatasetRow[] = [];

        let firstResultRow: DatasetRow = null;
        let index = -1;
        for (let value of this.datasetRows.entries()) {

            for (let key of keys.keys()) {
                let row = value[1]

                if (+row.getField(key).value != keys.get(key)) {
                    continue;
                }


                if (firstResultRow == null) {
                    index = value[0];
                    firstResultRow = row;
                }
                rows.push(row);
            }

        }

        if (rows.length == 0) {
            this.theCurrentRow = undefined;
            return {
                next(): IteratorResult<DatasetRow> {
                    return {
                        value: undefined,
                        done: true
                    }
                },

                [Symbol.iterator](): IterableIterator<DatasetRow> {
                    return this;
                }
            }
        }
        // Move to the first row of the result set
        this.theCurrentRow = firstResultRow;
        this.iterationIndex = index;

        return rows.values();
        //rows.values();
    }

    public getField(fieldName: string): Field {
        console.log("rows : " + this.datasetRows.size);
        if (this.datasetRows.size == 0 || this.theCurrentRow == null) return undefined;

        return this.theCurrentRow.getField(fieldName);
    }

    public getValue(fieldName: string): string {
        if (this.datasetRows.size == 0 || this.theCurrentRow == null) return undefined;

        return this.theCurrentRow.getValue(fieldName);
    }

    public setFieldValue(fieldName: string, value: string): void {
        if (this.datasetRows.size == 0) throw new Error("Dataset has no rows.");
        if (this.theCurrentRow == null) throw new Error("Current row is null.");

        return this.theCurrentRow.setFieldValue(fieldName, value);
    }

    subscribe(observer: (v: DatasetEvent<DataRow, Field>) => void): Subscription {
        return this.theCurrentRow.subscribe(observer);
    }

    public isRowPopulated(): boolean {
        if (this.datasetRows.size == 0) throw new Error("Dataset has no rows.");
        if (this.theCurrentRow == null) throw new Error("Current row is null.");

        return this.theCurrentRow.isRowPopulated();
    }

    public resetModified() {
        if (this.datasetRows.size == 0) throw new Error("Dataset has no rows.");
        if (this.theCurrentRow == null) throw new Error("Current row is null.");

        this.theCurrentRow.resetModified();
    }

}

/**
 * Default implementation of the FieldDescriptor interface. It is used to describe a field in a dataset.
 */
export class DefaultFieldDescriptor implements FieldDescriptor {

    constructor(
        public readonly name: string,
        public readonly type: FieldType) {
    }

}

/**
 *
 */
export class TypedField implements Field {

    public readonly id: string = uuidv4();
    private readonly subject: Subject<DatasetEvent<Field, Field>> = new Subject<DatasetEvent<Field, Field>>();


    constructor(public readonly name: string, private fieldValue: string, public readonly type: FieldType, private isModified? : boolean) {
    }

    get value(): string {
        return this.fieldValue;
    }

    set value(value: string) {
        this.fieldValue = value;
        this.isModified = true;

        this.subject.next(new DatasetEvent<Field, Field>(this.id, this, this, DatasetEventType.FIELD_VALUE_UPDATED));
    }

    public get modified() {
        return this.isModified;
    }

    public resetModified(): void {
        this.isModified = false;
    }

    public subscribe(observer: (e: DatasetEvent<Field, Field>) => void): Subscription {
        return this.subject.subscribe(observer);
    }

}

/**
 * Returns a hash value for the given field descriptors.
 */
export function calculateTypeHash(descriptors: Map<string, FieldDescriptor>): number {

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

/**
 * The DatasetRow class represents a row in a dataset.
 */
export class DatasetRow implements DataRow {

    public readonly id: string;
    private readonly subject: Subject<DatasetEvent<DatasetRow, Field>> = new Subject<DatasetEvent<DatasetRow, Field>>();
    private fieldDescriptors: Map<string, FieldDescriptor> = new Map<string, FieldDescriptor>();
    private datasetFieldMap: Map<string, TypedField> = new Map<string, TypedField>();
    //private modifiedFlag: boolean = false;

    constructor(fieldDescriptors?: FieldDescriptor[]) {
        this.id = uuidv4();

        if (fieldDescriptors != null) {
            fieldDescriptors.forEach(value => {
                this.addFieldDescriptor(value);
                this.addField(value);
            });
        }
    }

    public get modified(): boolean {


        let tmpModified = false;
        this.datasetFieldMap.forEach((value: TypedField, key: string) => {
            if (value.modified) {
                tmpModified = true;
                return;
            }
        });

        return tmpModified;
    }

    get typeHash(): number {

        return calculateTypeHash(this.fieldDescriptors);

    }


    /**
     * Adds a field to the row.
     * @param fieldName
     * @param type
     */
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

    /**
     * Returns an iterator for the fields in the row.
     */
    public entries(): IterableIterator<TypedField> {
        return this.datasetFieldMap.values();
    }

    /**
     * Subscribes to the row for changes.
     * @param observer
     */
    public subscribe(observer: (v: DatasetEvent<DatasetRow, Field>) => void): Subscription {
        return this.subject.subscribe(observer);
    }

    public isRowPopulated(): boolean {

        let rowPopulated = true;

        this.fieldDescriptors.forEach((value: FieldDescriptor, key: string) => {
            if (!this.datasetFieldMap.has(key)) {
                rowPopulated = false;
            }
        });

        return rowPopulated;
    }

    public resetModified(): void {
        this.datasetFieldMap.forEach((value: TypedField, key: string) => {
            value.resetModified();
        });
    }

    private addField(value: FieldDescriptor): TypedField {

        let thisRow = this;
        let tf = new TypedField(value.name, "", value.type);

        this.datasetFieldMap.set(value.name, tf);

        // Make the row an observer of the field, and fire an event if the field value changes.
        tf.subscribe((v: DatasetEvent<Field, Field>) => {

            thisRow.subject.next(
                new DatasetEvent<DatasetRow, Field>(thisRow.id, v.detail, thisRow, DatasetEventType.ROW_UPDATED)
            );
        });

        return tf;
    }

    private addFieldDescriptor(fieldDescription: FieldDescriptor): void {
        this.fieldDescriptors.set(fieldDescription.name, fieldDescription);
    }


}

/**
 * A Dataset is a collection of rows, each of which is a collection of fields.
 *
 * The constructor takes two arguments: fieldDescriptors and rows. The
 * fieldDescriptors argument is an array of objects that define the names
 * and types of the fields in the dataset. The rows argument is an optional
 * array of DatasetRow objects that represents the initial rows of the dataset.
 *
 * The dataset will emit events to its subscribers when it is modified.
 *
 * The Dataset class defines several methods that can be used to manipulate
 * the dataset. For example, the iterator method returns an iterator that
 * can be used to iterate over the rows in the dataset. The getRow method
 * returns a specific row from the dataset. The addField method adds a new
 * field to the dataset.
 *
 * The Dataset class also defines several getter and setter methods that
 * can be used to get and set properties of the dataset. For example, the
 * quiet getter and setter methods can be used to get and set the isQuiet
 * property of the dataset. The json_d getter method returns a JSON
 * representation of the dataset.
 *
 * Overall, the Dataset class provides a convenient way to represent
 * collections of rows and fields in TypeScript.
 */
export class Dataset implements DataRow {

    private readonly subject: Subject<DatasetEvent<Dataset, DatasetRow>> = new Subject<DatasetEvent<Dataset, DatasetRow>>();
    private readonly fieldDescriptorsMap: Map<string, FieldDescriptor> = new Map<string, FieldDescriptor>();
    private theRows: Map<number, DatasetRow> = new Map<number, DatasetRow>();
    private readonly datasetId: string;
    private readonly typeHash: number;
    private isQuiet: boolean = false;
    private modifiedFlag: boolean = false;

    private theNavigator: DatasetRowNavigator = null;

    private theDeletedRows: DatasetRow[] = [];

    constructor(fieldDescriptors: FieldDescriptors);

    constructor(fieldDescriptors: FieldDescriptors, rows?: DatasetRow[]) {
        this.datasetId = uuidv4();

        this.fieldDescriptorsMap = this.populateTheFieldDescriptorsMap(fieldDescriptors);
        this.typeHash = calculateTypeHash(this.fieldDescriptorsMap);
        this.populateDatasetWithRows(rows);
    }

    private populateDatasetWithRows(rows: DatasetRow[]) {
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

    private populateTheFieldDescriptorsMap(fieldDescriptors: { name: string; type: FieldType }[]): Map<string, FieldDescriptor> {
        let fieldDescMap = new Map<string, FieldDescriptor>();
        for (let descriptor of fieldDescriptors) {

            fieldDescMap.set(descriptor.name, descriptor);
        }

        return fieldDescMap;
    }

    public get quiet() {
        return this.isQuiet;
    }

    /**
     * Sets the quiet flag. If true, no events are emitted.
     * @param b
     */
    public set quiet(b) {
        if (b) {
            console.log("Quiet");
        } else {
            console.log("Loud");
        }
        this.isQuiet = b;
    }

    public get deletedRows(): DatasetRow[] {
        return this.theDeletedRows;
    }

    /**
     * Returns a clone of the field descriptors
     */
    get fieldDescriptors(): Map<string, FieldDescriptor> {

        let descriptors = new Map<string, FieldDescriptor>();

        for (let aDescriptor of this.fieldDescriptorsMap.values()) {

            descriptors.set(aDescriptor.name, aDescriptor);
        }

        return descriptors;
    }

    public get id(): string {
        return this.datasetId;
    }

    public get rowCount(): number {
        return this.theRows.size;
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

    public get modified(): boolean {
        this.modifiedFlag = false;

        this.theRows.forEach((row) => {
            if (row.modified) {
                this.modifiedFlag = true;
                return this.modifiedFlag
            }
        });

        return false;
    }

    public clearDeletedRows() {
        this.theDeletedRows = [];
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

        if (typeof rowId == "number") {
            return this.theRows.get(rowId);
        }

        for (let row of this.theRows.values()) {

            if (row.id === rowId) {
                return row;
            }

        }
        return null;
    }

    public getCurrentRow(): DatasetRow {
        return this.navigator().current().value;
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

            let fieldDescriptorsTmp = new Array<DefaultFieldDescriptor>();
            for (let value of this.fieldDescriptorsMap.values()) {

                fieldDescriptorsTmp.push(value);

            }
            row = new DatasetRow(fieldDescriptorsTmp);

        }

        let noRows = this.theRows.size
        this.theRows.set(noRows, row);
        this.navigator().last();

        // Make the dataset an observer of the row, and fire an event if there is a change to the row
        row.subscribe( (v) => {

            if (this.quiet) return;

            datasetThis.subject.next(new DatasetEvent<Dataset, DatasetRow>(datasetThis.datasetId, row, datasetThis, DatasetEventType.ROW_UPDATED));
        });
        if (!this.quiet) this.subject.next(new DatasetEvent<Dataset, DatasetRow>(this.datasetId, row, this, DatasetEventType.ROW_INSERTED));
        return row;
    }

    public deleteRow(rowId: string | number): DatasetRow {
        /*
        Deletion is an expensive operation as the order of the dataset is managed via a map of integers to rows. Removal
        of a row subsequently needs the map to be partially rebuilt.
         */

        let maxIndex = this.theRows.size - 1;

        let theRow = null;
        let key = -1;
        if (typeof rowId === "number") {

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


        if (theRow == undefined || theRow == null) {
            return null;
        }

        let lastRowDeleted = key == this.theRows.size - 1;
        this.theDeletedRows.push(theRow);
        this.theRows.delete(key);


        // Remap the remainder
        if (!lastRowDeleted) {
            for (let i = key; i < maxIndex; i++) {
                let nextRow = this.theRows.get(i + 1);
                this.theRows.set(i, nextRow);
            }
            this.theRows.delete(this.theRows.size - 1);

        }
        this.navigator().moveTo(key - 1);

        if (!this.quiet) this.subject.next(new DatasetEvent<Dataset, DatasetRow>(this.datasetId, theRow, this, DatasetEventType.ROW_DELETED));
    }

    public subscribe(observer: (v: DatasetEvent<Dataset, DataRow>) => void): Subscription {
        return this.subject.subscribe(observer);
    }

    public async load(pop: DataPump<Dataset>) {

        this.clear();

        console.log("Loading dataset");
        this.quiet = true;
        await pop.load(this).then(() => {
            this.quiet = false
        });
    }

    public clear() {
        this.theRows.clear();
        //this.fieldDescriptorsMap.clear();
    }

    public isRowPopulated(): boolean {
        return this.navigator().isRowPopulated();
    }

    public resetModified() {
        this.navigator().resetModified();
    }

}

/**
 * The ObjectArrayDataPump class is used to load data from an array of objects into a dataset.
 */
export class ObjectArrayDataPump implements DataPump<Dataset> {


    constructor(protected data: {}[]) {
    }

    public load(dataset: Dataset): Promise<LoadStatus> {

        return new Promise((resolve, reject) => {
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

            resolve(LoadStatus.SUCCESS);
        });
    }
}

export class DefaultObserver<SourceType, DetailType> {

    public detail: DetailType;
    public id: string;
    public source: SourceType;
    public count = 0;

    public get observer(): (v: DatasetEvent<SourceType, DetailType>) => void {
        return this.theObserver;
    }

    private theObserver: (v: DatasetEvent<SourceType, DetailType>) => void = (v: DatasetEvent<SourceType, DetailType>) => {

        this.id = v.id;
        this.detail = v.detail;
        this.source = v.source;
        this.count++;
    }
}

function resetModified() {
    throw new Error("Function not implemented.");
}
