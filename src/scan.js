"use strict";

var Int64 = require('node-int64');
const _ = require('underscore');
const serde = require('./serde');

class Scan {
    constructor(options) {
        this.startRow = (options && options.startRow);
        this.stopRow = (options && options.stopRow);
        this.numRows = (options && options.numRows) || 10;
        this.maxVersions = (options && options.maxVersions) || 1;
        this.filterString = (options && options.filterString);
        this.columns = [];
        this.columnTypes = {};
    }

    setStartRow(startRow) {
        this.startRow = startRow;
        return this;
    };

    setStopRow(stopRow) {
        this.stopRow = stopRow;
        return this;
    };

    setLimit(numRows) {
        this.numRows = numRows;
        return this;
    };

    add(family, qualifier, timestamp) {
        var familyMap = {};
        familyMap.family = family;
        if (qualifier) {
            if (typeof qualifier === 'object') {
                familyMap.qualifier = qualifier.name;
                const columnFullName = family + qualifier.name;
                this.columnTypes[columnFullName] = qualifier.type;
            }
            else {
                familyMap.qualifier = qualifier;
            }
        }

        if (timestamp) {
            familyMap.timestamp = new Int64(timestamp);
        }
        this.columns.push(familyMap);
        return this;
    };

    objectsFromData(hbaseRowsData) {
        if (_.isEmpty(this.columnTypes)) {
            return hbaseRowsData;
        }

        return _.map(hbaseRowsData, rowData => {
            const obj = {};
            obj.rowkey = rowData.row.toString();
            _.each(rowData.columnValues, colVal => {
                const family = colVal.family.toString();
                const qualName = colVal.qualifier.toString();
                const columnFullName = family + qualName;
                const columnType = this.columnTypes[columnFullName];
                obj[family] = obj[family] || {};
                obj[family][qualName] = serde.deserialize(colVal.value, columnType);
            });

            return obj;
        });
    }

    setMaxVersions(maxVersions) {
        if (maxVersions <= 0) {
            maxVersions = 1;
        }
        this.maxVersions = maxVersions;
        return this;
    };

    setFilterString(filterString) {
        this.filterString = filterString;
        return this;
    };
}

module.exports = Scan;