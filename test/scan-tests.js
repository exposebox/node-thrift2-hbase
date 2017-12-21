'use strict';

const _ = require('underscore');
const should = require('should');
const Promise = require('bluebird');

const config = require('./config');
const hbaseServiceCreate = require('../src/service');

const testStartMs = Date.now();
const generatedRowsCount = 2000;
const testTable = config.assets.testTableName;
const testQualifier = `q${testStartMs}`;

const testScanOptions = {
    columns: [{family: 'f', qualifier: testQualifier}]
};

describe('SCAN operation', function () {
    this.timeout(10000);

    before(async function () {
        const hbaseClient = hbaseServiceCreate(config.hbase);

        await putTestRows(hbaseClient, testStartMs);

        this.hbaseClient = hbaseClient;
    });

    after(function () {
        this.hbaseClient.destroy();
    });

    it('should get all rows', async function () {
        const rows = await scanRows({
            numRows: generatedRowsCount
        });

        should.equal(rows.length, generatedRowsCount);
    });

    it('should get range of rows', async function () {
        const rows = await scanRows({
            startRow: createRowKey(1300),
            stopRow: createRowKey(1600),
            numRows: generatedRowsCount * 2,
        });

        should.equal(rows.length, 300);
    });

    it.skip('should get all rows - 500 rows per iteration', async function () {
        return 1
    });

    it.skip('should get range of rows - 500 rows per iteration', async function () {
        return 1
    });

    let scanRows = async scanOptions => {
        const hbaseClient = this.ctx.hbaseClient;

        const scanObject = new hbaseClient.Scan(Object.assign({}, testScanOptions, scanOptions));

        console.log('Scanning rows...', scanObject);

        return hbaseClient.scanAsync(testTable, scanObject);
    };
});

const putTestRows = async function (hbaseClient) {
    console.log(`Putting ${generatedRowsCount} rows on table ${testTable}...`);

    await Promise.map(generateRange(1000, 1000 + generatedRowsCount), async generatedIndex => {
        const rowKey = createRowKey(generatedIndex);

        const putObject = new hbaseClient.Put(rowKey);
        putObject.add('f', testQualifier, {type: 'string', value: 't'});

        await hbaseClient.putAsync(testTable, putObject);
    });

    console.log('Put rows completed');
};

const createRowKey = index => `scan.${testStartMs}.${index}`;

const generateRange = function* (start, end) {
    for (let i = start; i < end; i++) {
        yield i;
    }
};