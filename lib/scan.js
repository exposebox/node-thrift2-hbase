"use strict";

var Int64 = require('node-int64');

function Scan() {
    if (!(this instanceof Scan)) {
        return new Scan();
    }
    this.startRow = 0;
    this.stopRow = 0;
    this.numRows = 10;
    this.maxVersions = 1;
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
    if(qualifier){
        familyMap.qualifier = qualifier;
    }
    if(timestamp){
        familyMap.timestamp = new Int64(timestamp);
    }
    this.familyList.push(familyMap);
    return this;
};

Scan.prototype.addFamily = function (family) {
    var familyMap = {};
    familyMap.family = family;
    this.familyList.push(familyMap);
    return this;
};

Scan.prototype.addColumn = function (family, qualifier) {
    var familyMap = {};
    familyMap.family = family;
    familyMap.qualifier = qualifier;
    this.familyList.push(familyMap);
    return this;
};

Scan.prototype.addTimestamp = function (family, qualifier, timestamp) {
    var familyMap = {};
    familyMap.family = family;
    familyMap.qualifier = qualifier;
    familyMap.timestamp = new Int64(timestamp);
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

//Get.prototype.getRow = function () {
//    return this.row;
//};
//
//Get.prototype.getMaxVersions = function () {
//    return this.maxVersions;
//};


module.exports = Scan;