import {DataPump, Dataset, DatasetRow, FieldDescriptors, ObjectArrayDataPump, PersistentDataPump} from "./dataset";
import * as fs from "fs";
import * as path from "path";

export class PersistentDataset extends Dataset {

    constructor(fieldDescriptors: FieldDescriptors, protected readonly persistentDataPump : PersistentDataPump) {
        super(fieldDescriptors);
    }

    public async load() {
        await super.load(this.persistentDataPump);
    }

    public async save() {
         await this.persistentDataPump.save(this);
    }
}

export class FilePersistentDataPump extends ObjectArrayDataPump {

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

    public save(dataset: Dataset): void {

        const json = JSON.stringify(dataset.json_d);

        let fileDescriptorId = fs.openSync(path.join(__dirname, this.filePath),'r+');
        fs.writeFileSync(path.join(__dirname, this.filePath), json);
        fs.closeSync(fileDescriptorId);


    }
}