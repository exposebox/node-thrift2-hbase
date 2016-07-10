"use strict";

var Int64 = require('node-int64');

function Put(row) {
    if (!(this instanceof Put)) {
        return new Put(row);
    }
    this.row = row;
    this.familyList = [];
}

Put.prototype.add = function (family, qualifier, value, timestamp) {
    var familyMap = {};
    familyMap.family = family;
    familyMap.qualifier = qualifier;
    familyMap.value = value;
    if(timestamp){
        familyMap.timestamp = new Int64(timestamp);
    }
    this.familyList.push(familyMap);
    return this;
};



module.exports = Put;