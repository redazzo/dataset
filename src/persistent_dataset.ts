import {
    DataPump,
    Dataset, DatasetRow, FieldDescriptors,
    ObjectArrayDataPump, PersistentDataPump
} from "./dataset";
import * as fs from "fs";
import * as path from "path";
import {v4 as uuidv4} from 'uuid';

export enum Comparator {
    EQ,
    LT,
    GT,
    ELT,
    EGT,
    LIKE

}

/**
 * PersistenceMode determines whether only modified fields and rows are written back to the database, or all fields are written back.
 */
export enum PersistenceMode {

    // Rows with modified fields are updated, and only the modified fields are written back
    BY_FIELD,

    // Rows with modified fields are updated, and all fields are written back
    BY_ROW,

    // All rows are updated and/or inserted, and all fields are written back
    BY_DATASET
}

/**
 * ReactiveWriteMode determines whether a field change will trigger a write to the database.
 */
export enum ReactiveWriteMode {
    DISABLED    = 0,   // Reactive writes are disabled
    ENABLED     = 1,   // Reactive writes are enabled
}

/**
 * DB_AUTO_KEY determines whether the database will generate a key for a new row.
 * If DB_AUTO_KEY is TRUE, it's expected that the database will generate a key for a new row.
 */
export enum DB_AUTO_KEY {
    FALSE,
    TRUE
}

/**
 * SaveMode determines whether a save operation will overwrite existing data, or will fail if the data has been modified since it was loaded.
 */
export enum SaveMode {
    OVERWRITE,
    OPTIMISTIC
}

export interface AutoKeyFactory {
    newKey() : string;
}

export class UUIDAutoKeyFactory implements AutoKeyFactory {
    newKey(): string {
        return uuidv4();
    }
}

/**
 * A persistent dataset is a dataset that can be saved and loaded from a persistent data source.
 */
export class PersistentDataset extends Dataset {

    constructor(fieldDescriptors: FieldDescriptors, protected readonly persistentDataPump : PersistentDataPump<PersistentDataset>) {
        super(fieldDescriptors);
    }

    public async load() {
        await super.load(this.persistentDataPump);
    }

    public async save() {
         await this.persistentDataPump.save(this);
    }

    public setLoadFilter(filter: Map<string, any>) {
        this.persistentDataPump.setLoadFilter(filter);
    }
}

/**
 * A keyed persistent dataset is a persistent dataset that has rows that can be identified by a key.
 */
export class KeyedPersistentDataset extends PersistentDataset {

    // Default key factory and auto key setting
    private keyFactory : AutoKeyFactory = new UUIDAutoKeyFactory();
    private dbAutoKeySetting : DB_AUTO_KEY = DB_AUTO_KEY.TRUE;

    constructor(
        fieldDescriptors: FieldDescriptors,
        protected readonly persistentDataPump: PersistentDataPump<KeyedPersistentDataset>,
        private readonly tableSource : {
            tableName : string,
            keys : string[]
        },  dbAutoKeySetting? : DB_AUTO_KEY ) {

        super(fieldDescriptors, persistentDataPump);

        if (dbAutoKeySetting != undefined) this.dbAutoKeySetting = dbAutoKeySetting;

        if (tableSource == null || tableSource == undefined) return;

        // keys must be at least one of the fields of the dataset
        let foundKey = false;
        let badKey = "";
        for (let key of tableSource.keys){
            for (let fieldDescriptor of fieldDescriptors){

                if (fieldDescriptor.name === key){
                    foundKey = true;
                    break;
                }
            }
            if (!foundKey) {
                badKey = key;
                break;
            }
        }

        if (!foundKey) throw Error("Key " + badKey + " is not specified as a field.")

    }

    public get dbAutoKey() : DB_AUTO_KEY {
        return this.dbAutoKeySetting;
    }

    public get source() : { tableName: string, keys : string[] } {
        return this.tableSource;
    }

    public addRow(row?: DatasetRow): DatasetRow {
        let newRow = super.addRow(row);

        // If the database is not generating the keys, then generate keys for the row
        if (this.dbAutoKeySetting == DB_AUTO_KEY.FALSE) {
            this.source.keys.forEach( (key) => {
                newRow[key] = this.keyFactory.newKey();
            });
        }
        return newRow;
    }

}

/**
 * A FilePersistentDataPump is a data pump that can load and save data from a file.
 */
export class FilePersistentDataPump<T extends PersistentDataset> extends ObjectArrayDataPump implements PersistentDataPump<T> {

    private loadFilter : Map<string, any> = new Map<string, any>();

    constructor(private filePath : string){
        let data = FilePersistentDataPump.readFromFile(filePath);
        super(data);
    }

    private static readFromFile(filePath : string) : {}[]  {
        let strData = fs.readFileSync(path.join(__dirname, filePath),'utf8');
        return JSON.parse(strData);
    }

    public reloadFile() {
        this.data = FilePersistentDataPump.readFromFile(this.filePath);
    }

    public save(dataset: Dataset): Promise<void> {

        return new Promise( () => {
            const json = JSON.stringify(dataset.json_d);

            let fileDescriptorId = fs.openSync(path.join(__dirname, this.filePath),'r+');
            fs.writeFileSync(path.join(__dirname, this.filePath), json);
            fs.closeSync(fileDescriptorId);
        });
    }

    public setLoadFilter(filter: Map<string, any>) {
        // TODO: Implement
    }
}