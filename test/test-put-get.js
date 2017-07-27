'use strict';

const should = require('should');
const assert = require('assert');

const Int64 = require('node-int64');
const _ = require('underscore');

const hbase = require('../src/service')({
    hosts: ["localhost"],
    port: "9090"
});

const testTable = "test:test";

describe('put tests', function () {
    this.timeout(5000);

    const putValues = {
        string: 'abcd',
        integer: 321,
        float: 321.654,
        number: Math.pow(2, 40) + 1,
        // long: new Int64('123456789abcdef0')
    };

    const bufferRead = {
        string: buf => buf.toString('utf8', 0, 4),
        integer: buf => buf.readInt32BE(),
        float: buf => buf.readFloatBE(),
        number: buf => buf.readIntBE(0, 6),
        long: bug => new Int64(buf)
    };

    _.each(putValues, function (expectedValue, valueType) {
        var testTitle = 'should put a ' + valueType + ' value (' + expectedValue + ')';
        it(testTitle, function () {
            console.log(testTitle);
            const rowKey = 'put-' + valueType;
            const valuesMap = {};
            valuesMap[valueType] = {type: valueType, value: expectedValue};
            console.log('putting row...');
            return hbase.putRowAsync(testTable, rowKey, 'f', valuesMap)
                .delay(1500)
                .then(function () {
                    console.log('reading written row...');
                    return hbase.getRowAsync(testTable, rowKey, ['f'], {})
                        .then(function (rowData) {
                            console.log('comparing values...', rowData);
                            should.exist(rowData);
                            rowData.should.not.be.empty;
                            var valueFromGet = rowData.columnValues[0].value;
                            console.log(valueFromGet);
                            console.log('expected:', expectedValue)
                            console.log('received:', bufferRead[valueType](valueFromGet))
                            // assert.(bufferRead[valueType](valueFromGet) , expectedValue);
                        });
                });
        });
    });
});