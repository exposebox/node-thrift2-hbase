'use strict';

const should = require('should');
const assert = require('assert');

const Int64 = require('node-int64');
const _ = require('underscore');
const serde = require('../src/serde');

const config = require('./config');
const hbaseServiceCreate = require('../src/service');

const testTable = "test:test";

describe('PUT operation', function () {
    this.timeout(5000);

    const putValues = {
        string: 'abcd',
        integer: 321,
        float: 1.5,
        double: 1024.2048,
        number: Math.pow(2, 34) + 1,
        // long: new Int64('123456789abcdef0')
    };

    before(function () {
        this.hbaseClient = hbaseServiceCreate(config.hbase);
    });

    after(function () {
        this.hbaseClient.destroy();
    });

    _.each(putValues, function (expectedValue, valueType) {
        const testTitle = `should put a ${valueType} value (${expectedValue})`;

        it(testTitle, async function () {
            const hbaseClient = this.hbaseClient;

            const rowKey = 'put-' + valueType;

            const putObject = new hbaseClient.Put(rowKey);
            putObject.add('f', valueType, {type: valueType, value: expectedValue});

            console.log('Putting row...', testTable, putObject);

            await hbaseClient.putAsync(testTable, putObject);

            const getObject = new hbaseClient.Get(rowKey);
            getObject.add('f', {name: valueType, type: valueType});

            console.log('Getting row...', getObject);

            const rowData = await hbaseClient.getAsync(testTable, getObject, {});

            should.equal(rowData && rowData.f && rowData.f[valueType], expectedValue);
        });
    });
});