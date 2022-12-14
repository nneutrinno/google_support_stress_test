"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
const Datastore = __importStar(require("@google-cloud/datastore"));
const _ = __importStar(require("lodash"));
const path = __importStar(require("path"));
const util = __importStar(require("util"));
const fs = __importStar(require("fs"));
const dotenv = require("dotenv");
const AMOUNT_FIELDS = 10;
const AMOUNT_CHARS_PER_FIELD = 100;
const DS_LIMIT = 500;
const PARALELISM = 20; // 36 // 18k
const NUMBER_OF_ENTITIES = 1000000;
const NUMBER_OF_CONNECTIONS_OPEN = 20;
const IS_USING_NESTED_ARRAY = true;
const NUMBER_OF_ARRAY_ELEMENTS = 10;
const NUMBER_OF_ARRAY_FIELDS_PER_ELEMENT = 10;
const AMOUNT_CHARS_PER_FIELD_IN_ARRAY = 20;
async function moveABunchOfData() {
    log(environment);
    log({
        AMOUNT_FIELDS,
        AMOUNT_CHARS_PER_FIELD,
        DS_LIMIT,
        PARALELISM,
        NUMBER_OF_ENTITIES,
        NUMBER_OF_CONNECTIONS_OPEN,
        IS_USING_NESTED_ARRAY,
        NUMBER_OF_ARRAY_ELEMENTS,
        NUMBER_OF_ARRAY_FIELDS_PER_ELEMENT,
        AMOUNT_CHARS_PER_FIELD_IN_ARRAY,
    });
    const entities = _.range(NUMBER_OF_ENTITIES).map(generateRow);
    let upsertArray = [];
    let count = 0;
    let iterations = 0;
    let total = 0;
    const generalStart = Date.now();
    TransactDatastore.initDataStore();
    try {
        let promises = [];
        let start = Date.now();
        for (const items of getSlicedArray(entities, DS_LIMIT)) {
            upsertArray.push(...items.map(row => getPayload(environment.KIND, row['id'], row)));
            // 
            if (isValidArray(upsertArray)) {
                ++count;
                promises.push(upsert(TransactDatastore.getDatastoreConnection, upsertArray));
                upsertArray = [];
            }
            if (count % PARALELISM === 0) {
                iterations++;
                const amount = count * DS_LIMIT;
                // 
                count = 0;
                await Promise.all(promises);
                const lapsed = Math.trunc((Date.now() - start) / 1000);
                total += amount;
                log('iterations', iterations, 'amount', amount, Math.trunc(amount / lapsed), 'per second', lapsed, 'seconds');
                start = Date.now();
                promises = [];
                if (iterations >= 50)
                    break;
            }
        }
        await Promise.all(promises);
        log("???? ~ file: datastore-stress-test.ts ~ line 291 ~ moveABunchOfData ~ out");
        const lapsed = Math.trunc((Date.now() - generalStart) / 1000);
        const average = Math.trunc(total / lapsed);
        log(average, 'per total second');
    }
    catch (err) {
        log('err', err);
    }
    function generateRow() {
        const mock = getMock();
        const row = { ...mock, id: SafeId.create(), [`field${1000}`]: mock };
        if (IS_USING_NESTED_ARRAY) {
            row.list = _.range(NUMBER_OF_ARRAY_ELEMENTS).map(() => getListMock());
        }
        return row;
    }
    function getGenericKey(datastore, kind, id) {
        return datastore.key([kind, id]);
    }
    function getPayload(kind, id, data) {
        return {
            key: getGenericKey(getDS(), kind, id),
            data
        };
    }
}
function getEnvironment() {
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
    });
}
async function upsert(getDS, upsertArray) {
    const ds = getDS();
    try {
        if (upsertArray.length > DS_LIMIT) {
            for (let array of getSlicedArray(upsertArray, DS_LIMIT)) {
                array.forEach((item) => {
                    item.excludeLargeProperties = true;
                });
                const datastore = ds;
                await datastore.upsert(array);
            }
            ;
        }
        else if (isValidArray(upsertArray)) {
            upsertArray.forEach((x) => {
                x.excludeLargeProperties = true;
            });
            const datastore = ds;
            await datastore.upsert(upsertArray);
        }
        ;
    }
    catch (err) {
        // log('upsert', {
        //     err,
        // })
        throw err;
    }
}
class TransactDatastore {
    static currentUsed;
    static dataStore;
    static initDataStore() {
        TransactDatastore.dataStore = [];
        TransactDatastore.currentUsed = 0;
        for (let k = 0; k < NUMBER_OF_CONNECTIONS_OPEN; ++k) {
            TransactDatastore.dataStore.push(getDS());
        }
    }
    ;
    static getDatastoreConnection() {
        if (++TransactDatastore.currentUsed >= NUMBER_OF_CONNECTIONS_OPEN) {
            TransactDatastore.currentUsed = 0;
        }
        return TransactDatastore.dataStore[TransactDatastore.currentUsed];
    }
    ;
}
function getDS() {
    return new Datastore.Datastore({
        projectId: environment.GCP_PROJECT_ID,
        apiEndpoint: '',
        namespace: environment.APP_ENV,
    });
}
// // // // // // // // 
function loadEnvironmentByName(name) {
    if (!fs.existsSync(path.join(__dirname, name)))
        throw new Error(`Missing ${name}`);
    dotenv.config({
        path: path.join(__dirname, name)
    });
}
function assertNonNullableFields(source) {
    for (const key in source)
        assertNonNullable(source[key], key);
}
function getNonNullableFields(source) {
    assertNonNullableFields(source);
    return source;
}
function getNonNullable(entity, name) {
    assertNonNullable(entity, name);
    return entity;
}
function assertNonNullable(entity, name) {
    if (!entity)
        throw new Error(`${name ?? 'Entity'} cannot be nullish`);
}
const base16Chars = "0123456789abcdef";
class SafeId {
    id;
    time;
    date;
    constructor(id) {
        this.id = id;
        if (this.id && this.id.length !== 24) {
            throw new Error('Invalid safe id');
        }
        const info = this.id ? SafeId.getInfoFromId(this.id) : SafeId.getSafeRandomIdInfo();
        this.id = SafeId.create(info);
        this.time = info.timestamp.time;
        this.date = info.timestamp.date;
    }
    static getInfoFromId(id) {
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
        };
    }
    toHexString() {
        return this.id;
    }
    static generateHexTimestamp() {
        const { hex } = this.generateTimestamp();
        return hex;
    }
    static getHex(time = Date.now()) {
        const hex = (time / 1000 | 0).toString(16);
        return hex;
    }
    static generateTimestamp() {
        const date = new Date();
        const time = date.getTime();
        const hex = SafeId.getHex(time);
        return {
            hex,
            time,
            date,
        };
    }
    getTimestamp() {
        return this.date;
    }
    getTime() {
        return this.time;
    }
    static getSafeRandomIdInfo() {
        const generated = SafeId.getGenerated();
        return {
            timestamp: SafeId.generateTimestamp(),
            generated,
        };
    }
    static getGenerated(size = 16) {
        const generated = getRandomIdentifier(size, SafeId.randomChar);
        return generated;
    }
    static randomChar() {
        return base16Chars[(Math.random() * 16 | 0)];
    }
    static create(info) {
        const hex = info?.timestamp.hex ?? SafeId.getHex();
        const generated = info?.generated ?? SafeId.getGenerated();
        return `${hex}${generated}`;
    }
}
function getRandomIdentifier(size, generator) {
    let out = '';
    while (out.length < size)
        out += generator();
    return out;
}
function getSlicedArray(array, slice) {
    const ret = [];
    const size = array.length;
    let k = 0;
    while (k < size) {
        const jump = k + slice < size ? k + slice : size;
        const offset = jump - k;
        ret.push(array.slice(k, jump));
        k += offset;
    }
    ;
    return ret;
}
;
const nullishSet = new Set([null, undefined]);
function isValidRef(value) {
    return !nullishSet.has(value);
}
function isInvalid(value) {
    return nullishSet.has(value);
}
function isValidArray(array, min = 1) {
    return isValidRef(array) && Array.isArray(array) && array.length >= min;
}
;
function defineMockCreator(amount, amountChars) {
    let mocked;
    return getMock;
    function getMock() {
        if (mocked)
            return mocked;
        const mock = createMock(amount, amountChars);
        mocked = mock;
        return mocked;
    }
}
let mocked;
const getMock = defineMockCreator(AMOUNT_FIELDS, AMOUNT_CHARS_PER_FIELD);
const getListMock = defineMockCreator(NUMBER_OF_ARRAY_FIELDS_PER_ELEMENT, AMOUNT_CHARS_PER_FIELD_IN_ARRAY);
function createMock(amount, amountChars) {
    const mock = {};
    _.range(amount).map(id => mock[`field${id}`] = SafeId.getGenerated(amountChars));
    return mock;
}
function print(...message) {
    return util.formatWithOptions({ colors: true, depth: null, showHidden: true, showProxy: true }, ...message);
}
function log(...message) {
    console.debug(print(...message));
}
// 
loadEnvironmentByName('.env');
const environment = getEnvironment();
moveABunchOfData();
