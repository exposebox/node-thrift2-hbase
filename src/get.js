"use strict";

var Int64 = require('node-int64');

function Get(row) {
    if (!(this instanceof Get)) {
        return new Get(row);
    }
    this.row = row;
    this.maxVersions = 1;
    this.familyList = [];
}

Get.prototype.add = function (family, qualifier, timestamp) {
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

Get.prototype.addFamily = function (family) {
    var familyMap = {};
    familyMap.family = family;
    this.familyList.push(familyMap);
    return this;
};

Get.prototype.addColumn = function (family, qualifier) {
    var familyMap = {};
    familyMap.family = family;
    familyMap.qualifier = qualifier;
    this.familyList.push(familyMap);
    return this;
};

Get.prototype.addTimestamp = function (family, qualifier, timestamp) {
    var familyMap = {};
    familyMap.family = family;
    familyMap.qualifier = qualifier;
    familyMap.timestamp = new Int64(timestamp);
    this.familyList.push(familyMap);
    return this;
};

Get.prototype.setMaxVersions = function (maxVersions) {
    if (maxVersions <= 0) {
        maxVersions = 1;
    }
    this.maxVersions = maxVersions;
    return this;
};


//Get.prototype.getRow = function () {
//    return this.row;
//};
//
//Get.prototype.getMaxVersions = function () {
//    return this.maxVersions;
//};


module.exports = Get;