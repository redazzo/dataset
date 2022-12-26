import {Observable, Subject, of, fromEvent, interval, Subscription} from "rxjs";
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


export class DatasetRow {

    private readonly subject : Subject<DatasetEvent<DatasetRow, Field>> = new Subject<DatasetEvent<DatasetRow, Field>>();
    private fieldDescriptors: Map<string, FieldDescriptor> = new Map<string, FieldDescriptor>();
    private datasetFieldMap: Map<string, TypedField> = new Map<string, TypedField>();
    public readonly id: string;


    constructor(fieldDescriptors? : FieldDescriptor[] ) {
        this.id = uuidv4();

        if (fieldDescriptors != null) {
            fieldDescriptors.forEach(value => {
                this.addFieldDescriptor(value);
            });

        }


    }

    public addColumn(fieldName : string, type : FieldType){
        let fieldDescriptor = new DefaultFieldDescriptor(fieldName, type);
        this.addFieldDescriptor(fieldDescriptor);
    }

    public getField(fieldName: string): TypedField {
        return this.datasetFieldMap.get(fieldName);
    }

    public getValue(fieldName: string) : string {
        return this.datasetFieldMap.get(fieldName).value;
    }

    public setFieldValue(fieldName: string, value: string) : void {

        let thisRow = this;

        let tf : TypedField = this.datasetFieldMap.get(fieldName);

        if ( tf == null || tf == undefined) {

            let fieldDescriptor = this.fieldDescriptors.get(fieldName);

            if (fieldDescriptor != null) {

                tf = new TypedField(fieldName, value, fieldDescriptor.type);

                // Make the row an observer of the field, and fire an event if the field value changes.
                tf.subscribe(  (v: DatasetEvent<Field, Field>) => {

                    thisRow.subject.next(

                        new DatasetEvent<DatasetRow, Field>(thisRow.id, v.detail, thisRow)
                    );
                });

                this.datasetFieldMap.set(fieldName, tf);

            } else {
                throw new Error("No such field.");
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

    public subscribe( observer : (v: DatasetEvent<DatasetRow, TypedField>) => void ) : Subscription {
        return this.subject.subscribe(observer);
    }


    get typeHash() : number {

        return calculateTypeHash(this.fieldDescriptors);

    }


}

export class Dataset {

    private readonly subject : Subject<DatasetEvent<Dataset, DatasetRow>> = new Subject<DatasetEvent<Dataset, DatasetRow>>();
    private readonly theFieldDescriptors: Map<string, FieldDescriptor> = new Map<string, FieldDescriptor>();
    private theRows: Array<DatasetRow> = new Array<DatasetRow>();
    private readonly datasetId: string;
    private readonly typeHash : number;

    constructor(fieldDescriptors : FieldDescriptors);

    constructor(fieldDescriptors : FieldDescriptors, rows? : DatasetRow[]) {
        this.datasetId = uuidv4();

        this.theFieldDescriptors = new Map<string, FieldDescriptor>();
        for (let descriptor of fieldDescriptors){

            this.theFieldDescriptors.set(descriptor.name, descriptor);
        }

        this.typeHash = calculateTypeHash(this.theFieldDescriptors);

        if (rows != null){

            for (let row of rows){

                if (row.typeHash != this.typeHash){
                    throw new Error("Rows in a dataset must have identical fields and field types.");
                }

                rows.push(row);
            }
        }
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

        if (this.theRows.length == 0) return null;

        for (let row of this.theRows){

            if (row.id === rowId ) {
                return row;
            }

        }
        return null;
    }

    public get rows() : DatasetRow[] {

        return this.theRows;

    }

    public getRowIds() : string[] {

        let rowIds = new Array<string>();

        for (let row of this.theRows){
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

        this.theRows.push(row);
        this.subject.next(new DatasetEvent<Dataset, DatasetRow>(this.datasetId, row, this));
    }

    get rowCount(): number {
        return this.theRows.length;
    }

    public subscribe( observer : (v: DatasetEvent<Dataset, DatasetRow>) => void ) : Subscription {
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

export function defaultPopulator<T extends Object>(data : T[] ) : (dataset : Dataset ) => void {

    return function defaultPopulateFunction( innerDataset) : void {

        for (let rowValues of data) {

            let row = new DatasetRow();
            let fieldDescriptors = innerDataset.fieldDescriptors;
            for (let entry of fieldDescriptors.values()){

                row.addColumn(entry.name, entry.type);
            }

            let mp : Map<string, string> = new Map<string, string>(Object.entries(JSON.parse(JSON.stringify(rowValues))));

            for (let key of mp.keys()){
                row.setFieldValue(key, mp.get(key));
            }

            innerDataset.addRow(row);

        }
    }

}

/*export function transformToArrayOfMaps( arrayOfMaps : {}[]) : Map<string, string>[] {

    let newArrayOfMaps = new Array<Map<string, string>>();

    for (let mapRows of arrayOfMaps) {

        let newMap = new Map<string, string>();

        for (let mp of mapRows) {

            let key = mp[0];
            let value = mp[1];
            newMap.set(key, value);

        }
        newArrayOfMaps.push(newMap);
    }

    return newArrayOfMaps;
}*/
