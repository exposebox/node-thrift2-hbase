"use strict";

var Int64 = require('node-int64');
const serde = require('./serde');

function Put(row) {
    if (!(this instanceof Put)) {
        return new Put(row);
    }
    this.row = row;
    this.columns = [];
}

Put.prototype.add = function (family, qualifier, value, timestamp) {
    var familyMap = {};
    familyMap.family = family;
    familyMap.qualifier = qualifier;
    familyMap.value = serde.serialize(value);

    if (timestamp) {
        familyMap.timestamp = new Int64(timestamp);
    }

    this.columns.push(familyMap);
    return this;
};


module.exports = Put;