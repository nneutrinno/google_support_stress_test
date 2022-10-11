import * as Datastore from '@google-cloud/datastore';
import * as _ from 'lodash';
import * as path from 'path';
import * as util from 'util';
import * as fs from 'fs';
import dotenv = require('dotenv');


function getEnvironment(): Env {
    const GCP_PROJECT_ID = process.env.GCP_PROJECT_ID;
    const APP_ENV = process.env.APP_ENV;
    const KIND = process.env.KIND;
    const GCP_CREDENTIALS_PATH = process.env.GCP_CREDENTIALS_PATH;

    process.env.GCLOUD_PROJECT = GCP_PROJECT_ID;
    process.env.GOOGLE_APPLICATION_CREDENTIALS = GCP_CREDENTIALS_PATH;

    return getNonNullableFields({
        GCP_PROJECT_ID,
        APP_ENV,
        DS_LIMIT,
        KIND,
        GCP_CREDENTIALS_PATH,
    })
}
async function moveABunchOfData(): Promise<void> {
    loadEnvironmentByName('.env');

    const environment = getEnvironment()

    const { APP_ENV, DS_LIMIT, GCP_PROJECT_ID, KIND } = environment;

    log(environment);
    
    const entities = _.range(NUMBER_OF_ENTITIES).map(generateRow);
    
    let upsertArray: DatastorePayload<Row>[] = [];
    let count: number = 0;
    const ds = getDS();

    try {
        let promises: Promise<void>[] = [];

        for (const items of getSlicedArray(entities, DS_LIMIT)) {
            upsertArray.push(...items.map(row => getPayload(KIND, row['id'], row)));

            if (isValidArray(upsertArray)) {
                ++count;
                promises.push(upsert(upsertArray));
                upsertArray = []
            }

            if (count % PARALELISM === 0) {
                count = 0;
                await Promise.all(promises)
                promises = [];
            }
        }

        await Promise.all(promises)

        log("🚀 ~ file: datastore-stress-test.ts ~ line 291 ~ moveABunchOfData ~ out");
        const items = await Promise.all(entities.map(item => ds.get(getGenericKey(ds, KIND, item.id))));
        log('Saved', items.filter(isValidRef).length)
        
    } catch (err) {
        log('err', err);
    }
    
    function generateRow(): Row {
        const row: Row = { ...getMock(), id: SafeId.create() };
        return row;
    }
    
    
    function getDS() {
        return new Datastore.Datastore({
            projectId: GCP_PROJECT_ID,
            apiEndpoint: '',
            namespace: APP_ENV,
        })
    }
    function stringKey(key: TDatastoreKey): string {
        return key.name + key.kind;
    }
    function getNonRepeatedUpsertKey(upsertArray: TArrayUpsert): TArrayUpsert {
        const nonRepeatedArray: TArrayUpsert = [];
        const unique: { [kindIDName: string]: TDatastorePayload } = {};
        for (const single of upsertArray) {
            unique[stringKey(single.key)] = single;
        };
        for (const key in unique) {
            const datastoreObj = unique[key];
            datastoreObj.excludeFromIndexes = [];
            nonRepeatedArray.push(datastoreObj);
        };
        return nonRepeatedArray;
    };
    async function upsert(upsertArray: TArrayUpsert): Promise<void> {
        const nonRepeatedArray: DatastorePayload[] = getNonRepeatedUpsertKey(upsertArray);

        try {

            if (nonRepeatedArray.length > DS_LIMIT) {
                for (let array of getSlicedArray<TDatastorePayload>(nonRepeatedArray, DS_LIMIT)) {
                    array.forEach((item) => {
                        item.excludeLargeProperties = true;
                    });
                    const datastore = ds;
                    await datastore.upsert(array);

                };
            } else if (isValidArray(nonRepeatedArray)) {
                nonRepeatedArray.forEach((x) => {
                    x.excludeLargeProperties = true;
                })
                const datastore = ds;
                await datastore.upsert(nonRepeatedArray);
            };

        } catch (err) {
            log('upsert', {
                err,
            })
            throw err;
        }
    }

    function getGenericKey(datastore: TDatastore, kind: string, id: string): TDatastoreKey {
        return datastore.key([kind, id]);
    }
    function getPayload<T extends {}>(kind: string, id: string, data: T): DatastorePayload<T> {
        return {
            key: getGenericKey(ds, kind, id),
            data
        }
    }
}


// // // // // // // // 

function loadEnvironmentByName(name: string) {
    if (!fs.existsSync(path.join(__dirname, name))) throw new Error(`Missing ${name}`)

    dotenv.config({
        path: path.join(__dirname, name)
    });
}

type NamedString<Name extends string> = string & CCustomType<Name>
type SafeIdText = NamedString<'SafeId'>;

declare class CCustomType<Information> {
    static readonly type: unique symbol;
    private [CCustomType.type]?: Information;
}

type AssertNonNullableFields<T extends {}> = { [key in keyof T]-?: NonNullable<T[key]> };

function assertNonNullableFields<T extends {}>(source: T): asserts source is AssertNonNullableFields<T> {
    for (const key in source) assertNonNullable(source[key], key);
}
function getNonNullableFields<T extends {}>(source: T): AssertNonNullableFields<T> {
    assertNonNullableFields(source)
    return source;
}
function getNonNullable<T>(entity: T | null | undefined, name?: string): T {
    assertNonNullable(entity, name);
    return entity;
}

function assertNonNullable<T>(entity: T | null | undefined, name?: string): asserts entity is T {
    if (!entity) throw new Error(`${name ?? 'Entity'} cannot be nullish`);
}

const base16Chars = "0123456789abcdef";

class SafeId {

    private time: number;
    private date: Date;

    constructor(
        public id?: SafeIdText,
    ) {
        if (this.id && this.id.length !== 24) {
            throw new Error('Invalid safe id');
        }
        const info = this.id ? SafeId.getInfoFromId(this.id) : SafeId.getSafeRandomIdInfo();
        this.id = SafeId.create(info);
        this.time = info.timestamp.time;
        this.date = info.timestamp.date;
    }

    static getInfoFromId(id: SafeIdText): SafeIdInfo {
        const hexTimestamp = id.slice(0, 8);
        const time = parseInt(hexTimestamp, 16) * 1000;
        const date = new Date(time);
        const generated = id.slice(8);

        return {
            generated,
            timestamp: {
                date,
                time,
                hex: hexTimestamp,
            }
        }
    }

    public toHexString() {
        return this.id;
    }

    public static generateHexTimestamp() {
        const { hex } = this.generateTimestamp()
        return hex;
    }

    static getHex(time: number = Date.now()) {
        const hex = (time / 1000 | 0).toString(16);
        return hex
    }

    public static generateTimestamp() {
        const date = new Date()
        const time = date.getTime()
        const hex = SafeId.getHex(time);

        return {
            hex,
            time,
            date,
        };
    }

    public getTimestamp(): Date {
        return this.date;
    }

    public getTime(): number {
        return this.time;
    }

    public static getSafeRandomIdInfo(): SafeIdInfo {
        const generated = SafeId.getGenerated();

        return {
            timestamp: SafeId.generateTimestamp(),
            generated,
        };
    }

    static getGenerated(size: number = 16) {
        const generated = getRandomIdentifier(size, SafeId.randomChar);
        return generated;
    }

    static randomChar() {
        return base16Chars[(Math.random() * 16 | 0)];
    }

    public static create(info?: SafeIdInfo): SafeIdText {
        const hex = info?.timestamp.hex ?? SafeId.getHex();
        const generated = info?.generated ?? SafeId.getGenerated();
        return `${hex}${generated}`;
    }

}

function getRandomIdentifier(size: number, generator: () => string ) {
    let out: string = '';
    while (out.length < size) out += generator();
    return out;
}


interface SafeIdInfo {
    timestamp: {
        hex: string;
        time: number;
        date: Date;
    };
    generated: string;
}

function getSlicedArray<T>(array: Array<T>, slice: number): Array<Array<T>> {
    const ret: Array<Array<T>> = [];
    const size: number = array.length;
    let k: number = 0;
    while (k < size) {
        const jump: number = k + slice < size ? k + slice : size;
        const offset: number = jump - k;
        ret.push(array.slice(k, jump));
        k += offset;
    };
    return ret;
};


type Nullish = undefined | null;
const nullishSet = new Set<Nullish>([null, undefined])

function isValidRef<T>(value: T): value is Exclude<T, Nullish> {
    return !nullishSet.has(value as any)
}
function isInvalid(value: any): value is Nullish {
    return nullishSet.has(value)
}

function isValidArray<T>(array: T, min: number = 1): boolean {
    return isValidRef(array) && Array.isArray(array) && array.length >= min;
}

interface TDatastoreKey {
    namespace?: string;
    id?: string;
    name?: string;
    kind: string;
    parent?: TDatastoreKey;
    path: Array<string | number>;
    /**
     * Access the `serialized` property for a library-compatible way to re-use a
     * key.
     *
     * @returns {object}
     *
     * @example
     * const key = datastore.key({
     *   namespace: 'My-NS',
     *   path: ['Company', 123]
     * });
     *
     * // Later...
     * const key = datastore.key(key.serialized);
     */
    readonly serialized: {
        namespace: string | undefined;
        path: (string | any)[];
    };
};

interface DatastorePayload<T extends {} = {}> {
    key: TDatastoreKey;
    // TODO Include possibility of 'raw data' with indexing options, etc
    data: T;
    excludeFromIndexes?: string[];
    excludeLargeProperties?: true
}


type TDatastorePayload = DatastorePayload
type TArrayUpsert = TDatastorePayload[]
type TDatastore = Datastore.Datastore;

interface Env {
    GCP_PROJECT_ID: string;
    APP_ENV: string;
    DS_LIMIT: number;
    KIND: string;
    GCP_CREDENTIALS_PATH: string;
}

const AMOUNT_FIELDS: number = 50;
const AMOUNT_CHARS_PER_FIELD: number = 100;
const DS_LIMIT: number = 500
const PARALELISM: number = 300
const NUMBER_OF_ENTITIES: number = 100000




let mocked: (Mock | undefined);

function getMock() {
    if (mocked) return mocked;

    const mock: Mock = {};
    _.range(AMOUNT_FIELDS).map(id => mock[`field${id}`] = SafeId.getGenerated(AMOUNT_CHARS_PER_FIELD));
    mocked = mock;

    return mocked;
}


type Row = & { id: string } & Mock
type Mock = & { [key in `field${number}`]: string }


function print(...message: unknown[]) {
    return util.formatWithOptions({ colors: true, depth: null, showHidden: true, showProxy: true }, ...message)
}
function log(...message: unknown[]): void {
    console.debug(print(...message));
}


// 

moveABunchOfData()