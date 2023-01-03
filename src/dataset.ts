import {Subject, Subscription} from "rxjs";
import {v4 as uuidv4} from 'uuid';

export enum FieldType {
    STRING,
    INTEGER,
    FLOAT,
}

export interface FieldDescriptor {

    get name(): string
    get type(): FieldType
}

export interface Field extends FieldDescriptor {

    get id(): string
    get value(): string
    subscribe( observer : (e: DatasetEvent<Field, Field>) => void ) : Subscription
}

export class DefaultFieldDescriptor implements FieldDescriptor {

    constructor(
        public readonly name : string,
        public readonly type : FieldType){
    }

}

export type FieldDescriptors = {
    name: string,
    type: FieldType
}[];


export class DatasetEvent<SourceType, DetailType> {

    constructor(
        public readonly id : string,
        public readonly detail : DetailType,
        public readonly source : SourceType ) {
    }
}

export class TypedField implements Field {

    public readonly id: string = uuidv4();
    private readonly subject : Subject<DatasetEvent<Field, Field>> = new Subject<DatasetEvent<Field, Field>>();


    constructor(public readonly name: string, private fieldValue: string, public readonly type: FieldType) {
    }

    get value(): string {
        return this.fieldValue;
    }

    set value(value: string) {
        this.fieldValue = value;
        this.subject.next(new DatasetEvent<Field, Field>(this.id, this, this));
    }

    public subscribe( observer : (e: DatasetEvent<Field, Field>) => void ) : Subscription {
        return this.subject.subscribe(observer);
    }

}

export interface DataRow {

    getField(fieldName: string): Field

    getValue(fieldName: string) : string

    setFieldValue(fieldName: string, value: string) : void

    subscribe( observer : (v: DatasetEvent<DataRow, Field | DataRow>) => void ) : Subscription

}


export class DatasetRow implements DataRow {

    private readonly subject : Subject<DatasetEvent<DatasetRow, Field>> = new Subject<DatasetEvent<DatasetRow, Field>>();
    private fieldDescriptors: Map<string, FieldDescriptor> = new Map<string, FieldDescriptor>();
    private datasetFieldMap: Map<string, TypedField> = new Map<string, TypedField>();
    public readonly id: string;

    constructor(fieldDescriptors? : FieldDescriptor[] ) {
        this.id = uuidv4();

        if (fieldDescriptors != null) {
            fieldDescriptors.forEach(value => {
                this.addFieldDescriptor(value);
                this.addField(value);
            });
        }
    }

    private addField(value: FieldDescriptor) : TypedField {

        let thisRow = this;
        let tf = new TypedField(value.name, "", value.type);

        // Make the row an observer of the field, and fire an event if the field value changes.
        tf.subscribe((v: DatasetEvent<Field, Field>) => {

            thisRow.subject.next(
                new DatasetEvent<DatasetRow, Field>(thisRow.id, v.detail, thisRow)
            );
        });

        this.datasetFieldMap.set(value.name, tf);
        return tf;
    }

    public addColumn(fieldName : string, type : FieldType){
        let fieldDescriptor = new DefaultFieldDescriptor(fieldName, type);
        this.addFieldDescriptor(fieldDescriptor);
        this.addField(fieldDescriptor);
    }

    public getField(fieldName: string): TypedField {
        return this.datasetFieldMap.get(fieldName);
    }

    public getValue(fieldName: string) : string {
        return this.datasetFieldMap.get(fieldName).value;
    }

    public setFieldValue(fieldName: string, value: string) : void {

        let tf : TypedField = this.datasetFieldMap.get(fieldName);

        if ( tf == null || tf == undefined) {

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


    private addFieldDescriptor(fieldDescription : FieldDescriptor): void {
        this.fieldDescriptors.set(fieldDescription.name, fieldDescription);
    }

    public subscribe( observer : (v: DatasetEvent<DatasetRow, Field>) => void ) : Subscription {
        return this.subject.subscribe(observer);
    }


    get typeHash() : number {

        return calculateTypeHash(this.fieldDescriptors);

    }


}

class DatasetRowNavigator implements NavigableIterator<DatasetRow> {

    private index : number = -1;
    private readonly maxSize: number;
    private theCurrentRow : DatasetRow;

    public constructor(private readonly datasetRows : Map<number, DatasetRow>) {
        this.maxSize = datasetRows.size;
        if (datasetRows.size > 0){
            this.theCurrentRow = datasetRows.get(0);
        }
    }

    public [Symbol.iterator](): IterableIterator<DatasetRow> {
        return this;
    }

    public first() : IteratorResult<DatasetRow, any> {
        if (this.datasetRows.size > 0){
            this.index = 0;
            this.theCurrentRow = this.datasetRows.get(this.index);
        } else {
            this.theCurrentRow = null;
        }

        return {
            value: this.theCurrentRow
        }
    }

    public prior() : IteratorResult<DatasetRow> {
        if (this.index > 0){
            this.index--;
            this.theCurrentRow = this.datasetRows.get(this.index);

            return {
                value: this.theCurrentRow
            }

        }

        return {
            value: undefined,
            done: true
        }


    }

    public current() : IteratorResult<DatasetRow> {
        return {
            value: this.theCurrentRow
        }
    }
    
    

    public next(...args: [] | [undefined]): IteratorResult<DatasetRow, any> {

        if (this.index < this.maxSize - 1) {

            this.index++;
            this.theCurrentRow = this.datasetRows.get(this.index);

            return {
                value: this.theCurrentRow
            }

        }

        return {
            value: undefined,
            done: true
        }
    }

    public getField(fieldName: string): Field {
        return this.theCurrentRow.getField(fieldName);
    }

    public getValue(fieldName: string) : string {
        return this.theCurrentRow.getValue(fieldName);
    }

    public setFieldValue(fieldName: string, value: string) : void {
        return this.theCurrentRow.setFieldValue(fieldName, value);
    }

    subscribe(observer: (v: DatasetEvent<DataRow, Field>) => void): Subscription {
        return this.theCurrentRow.subscribe(observer);
    }

}

export interface NavigableIterator<T> extends IterableIterator<T>, DataRow {

    first() : IteratorResult<T, any>
    prior() : IteratorResult<T, any>
    current() : IteratorResult<T,  any>

}

export class Dataset implements DataRow {

    private readonly subject : Subject<DatasetEvent<Dataset, DatasetRow>> = new Subject<DatasetEvent<Dataset, DatasetRow>>();
    private readonly theFieldDescriptors: Map<string, FieldDescriptor> = new Map<string, FieldDescriptor>();
    private theRows: Map<number, DatasetRow> = new Map<number, DatasetRow>();
    private readonly datasetId: string;
    private readonly typeHash : number;

    private theNavigator : DatasetRowNavigator = null;



    constructor(fieldDescriptors : FieldDescriptors);

    constructor(fieldDescriptors : FieldDescriptors, rows? : DatasetRow[]) {
        this.datasetId = uuidv4();

        this.theFieldDescriptors = new Map<string, FieldDescriptor>();
        for (let descriptor of fieldDescriptors){

            this.theFieldDescriptors.set(descriptor.name, descriptor);
        }

        this.typeHash = calculateTypeHash(this.theFieldDescriptors);

        if (rows != null){

            let noRows = 0;
            for (let row of rows){

                if (row.typeHash != this.typeHash){
                    throw new Error("Rows in a dataset must have identical fields and field types.");
                }

                this.theRows.set(noRows++, row);
            }
        }
    }

    public iterator() : IterableIterator<DatasetRow> {
        return new DatasetRowNavigator(this.theRows);
    }

    public navigator() : NavigableIterator<DatasetRow> {

        if (this.theNavigator == null){
            this.theNavigator = new DatasetRowNavigator(this.theRows);
        }
        return this.theNavigator;
    }

    public addColumn(fieldName : string, type : FieldType){

        let fd = this.theFieldDescriptors.get(fieldName);

        if (!(fd == null || fd == undefined)) {
            throw new Error("A column with that field name already exists!");
        }

        fd = new DefaultFieldDescriptor(fieldName, type);
        this.theFieldDescriptors.set(fieldName, fd);

    }

    public getField(fieldName: string): TypedField {
        let currentRow = this.navigator().current().value;
        return currentRow.getField(fieldName);
    }

    public getValue(fieldName: string) : string {
        let currentRow = this.navigator().current().value;
        return currentRow.getValue(fieldName);
    }

    public setFieldValue(fieldName: string, value: string) : void {
        let currentRow = this.navigator().current().value;
        currentRow.setFieldValue(fieldName, value);
    }

    /**
     * Returns a clone of the field descriptors
     */
    get fieldDescriptors(): Map<string, FieldDescriptor> {

        let descriptors = new Map<string, FieldDescriptor>();

        for (let aDescriptor of this.theFieldDescriptors.values()){

            descriptors.set(aDescriptor.name, aDescriptor);
        }

        return descriptors;
    }

    get id(): string {
        return this.datasetId;
    }


    public getRow( rowId : string ) : DatasetRow {

        if (this.theRows.size == 0) return null;

        for (let row of this.theRows.values()){

            if (row.id === rowId ) {
                return row;
            }

        }
        return null;
    }

    public getRowIds() : string[] {

        let rowIds = new Array<string>();

        for (let row of this.theRows.values()){
            rowIds.push(row.id);
        }

        return rowIds;
    }


    public addRow(row? : DatasetRow): void {

        let datasetThis = this;

        if (row == null || row == undefined) {

            let values = this.theFieldDescriptors.values();

            let fieldDescriptorsTmp = new Array<DefaultFieldDescriptor>();
            for (let value of values){

                fieldDescriptorsTmp.push(value);

            }
            row = new DatasetRow(fieldDescriptorsTmp);

        }

        // Make the dataset an observer of the row, and fire an event if there is a change to the row
        row.subscribe( ( v) => {

            datasetThis.subject.next(new DatasetEvent<Dataset, DatasetRow>(datasetThis.datasetId, row, datasetThis));
        });

        let noRows = this.theRows.size
        this.theRows.set(noRows, row);
        this.subject.next(new DatasetEvent<Dataset, DatasetRow>(this.datasetId, row, this));
    }

    get rowCount(): number {
        return this.theRows.size;
    }

    public subscribe( observer : (v: DatasetEvent<Dataset, DataRow>) => void ) : Subscription {
        return this.subject.subscribe(observer);
    }

    public populate( populator: ( dataSet : Dataset ) => void) : void {

        populator(this);

    }

}




/**
 * Returns a hash from the field descriptors. A bit crap - but good enough.
 */
function calculateTypeHash( descriptors : Map<string, FieldDescriptor> ) : number {

    let hash = 0;

    for (let descriptor of descriptors.values()){
        let name = descriptor.name + descriptor.type.toString();
        for (let i = 0; i < name.length; i++){

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

// Not the best ...
export function defaultPopulator(data : {}[] ) : (dataset : Dataset ) => void {

    return function defaultPopulateFunction( innerDataset) : void {

        for (let rowValues of data) {

            let row = new DatasetRow();
            let fieldDescriptors = innerDataset.fieldDescriptors;
            for (let entry of fieldDescriptors.values()){

                row.addColumn(entry.name, entry.type);
            }

            // Seems a bit dirty
            let mp : Map<string, string> = new Map<string, string>(Object.entries(JSON.parse(JSON.stringify(rowValues))));

            for (let key of mp.keys()){
                row.setFieldValue(key, mp.get(key));
            }

            innerDataset.addRow(row);

        }
    }

}
