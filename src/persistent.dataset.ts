import {DataPump, Dataset, DatasetRow, FieldDescriptors, PersistentDataPump} from "./dataset";
import * as fs from "fs";
import * as path from "path";

export class PersistentDataset extends Dataset {

    constructor(fieldDescriptors: FieldDescriptors, private readonly persistentDataPump : PersistentDataPump) {
        super(fieldDescriptors);
    }

    public async load() {
        await super.load(this.persistentDataPump);
    }

    public save() {
         this.persistentDataPump.save(this);
    }
}


export class FilePersistentDataPump implements PersistentDataPump {

    private readonly data: string;

    constructor(private filePath : string){

        let strData = fs.readFileSync(path.join(__dirname, filePath),'utf8');
        this.data = JSON.parse(strData);
    }

    load(dataset: Dataset): void {
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

    public save(dataset: Dataset): void {

        const json = JSON.stringify(dataset.json_d);

        let fileDescriptorId = fs.openSync(path.join(__dirname, this.filePath),'r+');
        fs.writeFileSync(path.join(__dirname, this.filePath), json);
        fs.closeSync(fileDescriptorId);


    }




}