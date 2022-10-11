const Datastore = require('@google-cloud/datastore');
const _ = require('lodash');
const path = require('path');
const util = require('util');
const fs = require('fs');


const AMOUNT_FIELDS = 50;
const AMOUNT_CHARS_PER_FIELD = 100;
const DS_LIMIT = 500;
const PARALELISM = 36; // 36 // 18k
const NUMBER_OF_ENTITIES = 100000;
async function moveABunchOfData() {
    loadEnvironmentByName('.env');
    const environment = getEnvironment();
    const { APP_ENV, DS_LIMIT, GCP_PROJECT_ID, KIND } = environment;
    log(environment);
    const entities = _.range(NUMBER_OF_ENTITIES).map(generateRow);
    let upsertArray = [];
    let count = 0;
    const ds = getDS();
    try {
        let promises = [];
        let start = Date.now();
        for (const items of getSlicedArray(entities, DS_LIMIT)) {
            upsertArray.push(...items.map(row => getPayload(KIND, row['id'], row)));
            // 
            if (isValidArray(upsertArray)) {
                ++count;
                promises.push(upsert(upsertArray));
                upsertArray = [];
            }
            if (count % PARALELISM === 0) {
                const amount = count * DS_LIMIT;
                // 
                count = 0;
                await Promise.all(promises);
                const lapsed = Math.trunc((Date.now() - start) / 1000);
                console.log('amount', amount, Math.trunc(amount / lapsed), 'per second', lapsed, 'seconds');
                start = Date.now();
                promises = [];
            }
        }
        await Promise.all(promises);
        log("🚀 ~ file: datastore-stress-test.ts ~ line 291 ~ moveABunchOfData ~ out");
        const items = await Promise.all(entities.map(item => ds.get(getGenericKey(ds, KIND, item.id))));
        log('Saved', items.filter(isValidRef).length);
    }
    catch (err) {
        log('err', err);
    }
    function generateRow() {
        const mock = getMock();
        const row = { ...mock, id: SafeId.create(), [`field${1000}`]: mock };
        return row;
    }
    function getDS() {
        return new Datastore.Datastore({
            projectId: GCP_PROJECT_ID,
            apiEndpoint: '',
            namespace: APP_ENV,
        });
    }
    function stringKey(key) {
        return key.name + key.kind;
    }
    function getNonRepeatedUpsertKey(upsertArray) {
        const nonRepeatedArray = [];
        const unique = {};
        for (const single of upsertArray) {
            unique[stringKey(single.key)] = single;
        }
        ;
        for (const key in unique) {
            const datastoreObj = unique[key];
            datastoreObj.excludeFromIndexes = [];
            nonRepeatedArray.push(datastoreObj);
        }
        ;
        return nonRepeatedArray;
    }
    ;
    async function upsert(upsertArray) {
        const nonRepeatedArray = getNonRepeatedUpsertKey(upsertArray);
        try {
            if (nonRepeatedArray.length > DS_LIMIT) {
                for (let array of getSlicedArray(nonRepeatedArray, DS_LIMIT)) {
                    array.forEach((item) => {
                        item.excludeLargeProperties = true;
                    });
                    const datastore = ds;
                    await datastore.upsert(array);
                }
                ;
            }
            else if (isValidArray(nonRepeatedArray)) {
                nonRepeatedArray.forEach((x) => {
                    x.excludeLargeProperties = true;
                });
                const datastore = ds;
                await datastore.upsert(nonRepeatedArray);
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
    function getGenericKey(datastore, kind, id) {
        return datastore.key([kind, id]);
    }
    function getPayload(kind, id, data) {
        return {
            key: getGenericKey(ds, kind, id),
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
let mocked;
function getMock() {
    if (mocked)
        return mocked;
    const mock = {};
    _.range(AMOUNT_FIELDS).map(id => mock[`field${id}`] = SafeId.getGenerated(AMOUNT_CHARS_PER_FIELD));
    mocked = mock;
    return mocked;
}
function print(...message) {
    return util.formatWithOptions({ colors: true, depth: null, showHidden: true, showProxy: true }, ...message);
}
function log(...message) {
    console.debug(print(...message));
}
// 
moveABunchOfData();
