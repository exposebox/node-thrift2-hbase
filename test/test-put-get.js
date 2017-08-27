'use strict';

const should = require('should');
const assert = require('assert');

const Int64 = require('node-int64');
const _ = require('underscore');
const serde = require('../src/serde');

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
        number: Math.pow(2, 34) + 1,
        // long: new Int64('123456789abcdef0')
    };

    _.each(putValues, function (expectedValue, valueType) {
        var testTitle = 'should put a ' + valueType + ' value (' + expectedValue + ')';
        it(testTitle, function () {
            console.log(testTitle);
            const rowKey = 'put-' + valueType;

            const valuesMap = {};
            valuesMap[valueType] = {type: valueType, value: expectedValue};

            const getObject = new hbase.Get(rowKey);
            let qualObject = {name: valueType, type: valueType};
            getObject.add('f', qualObject);

            console.log('putting row...');
            return hbase.putRowAsync(testTable, rowKey, 'f', valuesMap)
                .delay(1500)
                .then(function () {
                    console.log('reading written row...');
                    return hbase.getAsync(testTable, getObject, {})
                        .then(function (rowData) {
                            should.exist(rowData);
                            rowData.should.not.be.empty;
                            console.log('expected:', expectedValue);
                            console.log('result object:', rowData);
                            // assert.(bufferRead[valueType](valueFromGet) , expectedValue);
                        });
                });
        });
    });
});