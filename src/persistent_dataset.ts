import {
    DataPump,
    Dataset, FieldDescriptors,
    ObjectArrayDataPump, PersistentDataPump
} from "./dataset";
import * as fs from "fs";
import * as path from "path";

export enum Comparator {
    EQ,
    LT,
    GT,
    ELT,
    EGT,
    LIKE

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
}

/**
 * A keyed persistent dataset is a persistent dataset that has rows that can be identified by a key.
 */
export class KeyedPersistentDataset extends PersistentDataset {

    constructor(
        fieldDescriptors: FieldDescriptors,
        protected readonly persistentDataPump: PersistentDataPump<KeyedPersistentDataset>,
        private readonly tableSource? : {
            tableName : string,
            keys : string[]
        }) {

        super(fieldDescriptors, persistentDataPump);

        if (tableSource == null || tableSource == undefined) return;

        // keys must be one of the fields
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

    public get source() : { tableName: string, keys : string[] } {
        return this.tableSource;
    }

}

/**
 * A FilePersistentDataPump is a data pump that can load and save data from a file.
 */
export class FilePersistentDataPump<T extends PersistentDataset> extends ObjectArrayDataPump implements PersistentDataPump<T> {

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
}