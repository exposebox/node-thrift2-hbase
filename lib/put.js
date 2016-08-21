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

    if (typeof  value === "object") {
        if (value.type === "integer") {
            var buf = new Buffer(4);
            buf.writeInt32BE(value.value, 0);
            familyMap.value = buf;
        } else if (value.type === "float") {
            var buf = new Buffer(4);
            buf.writeFloatBE(value.value, 0);
            familyMap.value = buf;
        } else {
            throw new Error('Unsupported value.type for put qualifier ' + family + ':' + qualifier + ' , Value: ' + JSON.stringify(value));
        }
    } else {
        familyMap.value = value.toString();
    }

    if (timestamp) {
        familyMap.timestamp = new Int64(timestamp);
    }
    this.familyList.push(familyMap);
    return this;
};


module.exports = Put;