"use strict";

var Int64 = require('node-int64');

function Scan(options) {
    if (!(this instanceof Scan)) {
        return new Scan();
    }
    this.startRow = (options && options.startRow);
    this.stopRow = (options && options.stopRow);
    this.numRows = (options && options.numRows) || 10;
    this.maxVersions = (options && options.maxVersions) || 1;
    this.filterString = (options && options.filterString);
    this.familyList = [];
}

Scan.prototype.setStartRow = function (startRow) {
    this.startRow = startRow;
    return this;
};

Scan.prototype.setStopRow = function (stopRow) {
    this.stopRow = stopRow;
    return this;
};

Scan.prototype.setLimit = function (numRows) {
    this.numRows = numRows;
    return this;
};

Scan.prototype.add = function (family, qualifier, timestamp) {
    var familyMap = {};
    familyMap.family = family;
    if (qualifier) {
        familyMap.qualifier = qualifier;
    }
    if (timestamp) {
        familyMap.timestamp = new Int64(timestamp);
    }
    this.familyList.push(familyMap);
    return this;
};

Scan.prototype.setMaxVersions = function (maxVersions) {
    if (maxVersions <= 0) {
        maxVersions = 1;
    }
    this.maxVersions = maxVersions;
    return this;
};

Scan.prototype.setFilterString = function (filterString) {
    this.filterString = filterString;
    return this;
};

module.exports = Scan;