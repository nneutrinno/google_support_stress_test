import * as Datastore from '@google-cloud/datastore';
import * as _ from 'lodash';
import * as path from 'path';
import * as util from 'util';
import * as fs from 'fs';
import dotenv = require('dotenv');





const AMOUNT_FIELDS = 10;
const AMOUNT_CHARS_PER_FIELD = 100;
const DS_LIMIT = 500;
const PARALELISM = 5; // 36 // 18k
const NUMBER_OF_ENTITIES = 1000000;
const NUMBER_OF_CONNECTIONS_OPEN = 20;


const IS_USING_NESTED_ARRAY = true;
const NUMBER_OF_ARRAY_ELEMENTS = 10;
const NUMBER_OF_ARRAY_FIELDS_PER_ELEMENT = 10;
const AMOUNT_CHARS_PER_FIELD_IN_ARRAY = 20;


async function moveABunchOfData(): Promise<void> {
    log(environment);
    log({
        AMOUNT_FIELDS,
        AMOUNT_CHARS_PER_FIELD,
        DS_LIMIT,
        PARALELISM,
        NUMBER_OF_ENTITIES,
        NUMBER_OF_CONNECTIONS_OPEN,
    });

    const entities = _.range(NUMBER_OF_ENTITIES).map(generateRow);

    let upsertArray: DatastorePayload<Row>[] = [];
    let count: number = 0;
    let iterations: number = 0;
    let total: number = 0;
    const generalStart = Date.now();
    
    TransactDatastore.initDataStore();

    try {
        let promises: Promise<void>[] = [];
        let start = Date.now()

        for (const items of getSlicedArray(entities, DS_LIMIT)) {
            upsertArray.push(...items.map(row => getPayload(environment.KIND, row['id'], row)));

            // 

            if (isValidArray(upsertArray)) {
                ++count;
                promises.push(upsert(TransactDatastore.getDatastoreConnection, upsertArray));
                upsertArray = []
            }

            if (count % PARALELISM === 0) {
                iterations++;
                const amount = count * DS_LIMIT;
                // 
                count = 0;
                await Promise.all(promises);
                const lapsed = Math.trunc((Date.now() - start) / 1000);
                total += amount;
                log(
                    'iterations',
                    iterations,
                    'amount',
                    amount,
                    Math.trunc(amount / lapsed), 'per second',
                    lapsed, 'seconds'
                );
                start = Date.now();

                promises = [];
                if (iterations >= 50) break;
            }
        }

        await Promise.all(promises)

        log("🚀 ~ file: datastore-stress-test.ts ~ line 291 ~ moveABunchOfData ~ out");
        
        const lapsed = Math.trunc((Date.now() - generalStart) / 1000);
        const average = Math.trunc(total / lapsed);
        
        log(average, 'per total second');
        
    } catch (err) {
        log('err', err);
    }
    
    function generateRow(): Row {
        const mock = getMock();

        const row: Row = { ...mock, id: SafeId.create(), [`field${1000}`]: mock };
        if (IS_USING_NESTED_ARRAY) {
            row.list = _.range(NUMBER_OF_ARRAY_ELEMENTS).map(() => getListMock());
        }
        return row;
    }
    

    function getGenericKey(datastore: TDatastore, kind: string, id: string): TDatastoreKey {
        return datastore.key([kind, id]);
    }

    function getPayload<T extends {}>(kind: string, id: string, data: T): DatastorePayload<T> {
        return {
            key: getGenericKey(getDS(), kind, id),
            data
        }
    }
}

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

async function upsert(getDS: () => Datastore.Datastore, upsertArray: TArrayUpsert): Promise<void> {
    const ds = getDS();

    try {

        if (upsertArray.length > DS_LIMIT) {
            for (let array of getSlicedArray<TDatastorePayload>(upsertArray, DS_LIMIT)) {
                array.forEach((item) => {
                    item.excludeLargeProperties = true;
                });
                const datastore = ds;
                await datastore.upsert(array);

            };
        } else if (isValidArray(upsertArray)) {
            upsertArray.forEach((x) => {
                x.excludeLargeProperties = true;
            })
            const datastore = ds;
            await datastore.upsert(upsertArray);
        };
    } catch (err) {
        // log('upsert', {
        //     err,
        // })
        throw err;
    }
}
class TransactDatastore {
    private static currentUsed: number;
    private static dataStore: Datastore.Datastore[];

    public static initDataStore(): void {
        TransactDatastore.dataStore = [];
        TransactDatastore.currentUsed = 0;

        for (let k = 0; k < NUMBER_OF_CONNECTIONS_OPEN; ++k) {
            TransactDatastore.dataStore.push(getDS());
        }
    };

    public static getDatastoreConnection(): TDatastore {
        if (++TransactDatastore.currentUsed >= NUMBER_OF_CONNECTIONS_OPEN) {
            TransactDatastore.currentUsed = 0;
        }
        return TransactDatastore.dataStore[TransactDatastore.currentUsed];
    };
}


function getDS() {
    return new Datastore.Datastore({
        projectId: environment.GCP_PROJECT_ID,
        apiEndpoint: '',
        namespace: environment.APP_ENV,
    })
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



function defineMockCreator(amount: number, amountChars: number) {
    let mocked: (Mock | undefined);

    return getMock;

    function getMock() {
        if (mocked) return mocked;

        const mock: Mock = createMock(amount, amountChars);
        mocked = mock;

        return mocked;
    }
}

let mocked: (Mock | undefined);



const getMock = defineMockCreator(
    AMOUNT_FIELDS,
    AMOUNT_CHARS_PER_FIELD,
);



const getListMock = defineMockCreator(
    NUMBER_OF_ARRAY_FIELDS_PER_ELEMENT,
    AMOUNT_CHARS_PER_FIELD_IN_ARRAY
);


function createMock(amount: number, amountChars: number) {
    const mock: Mock = {};
    _.range(amount).map(id => mock[`field${id}`] = SafeId.getGenerated(amountChars));
    return mock;
}

type Row = & { id: string } & Mock & { list?: object[] }
type Mock = & { [key in `field${number}`]: string }


function print(...message: unknown[]) {
    return util.formatWithOptions({ colors: true, depth: null, showHidden: true, showProxy: true }, ...message)
}
function log(...message: unknown[]): void {
    console.debug(print(...message));
}


// 


loadEnvironmentByName('.env');
const environment = getEnvironment();
moveABunchOfData();