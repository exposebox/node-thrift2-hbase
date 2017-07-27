"use strict";

var Int64 = require('node-int64');

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

    if (typeof  value === "object") {
        switch (value.type) {
            case "string":
                familyMap.value = value.value.toString();
                break;
            case "integer":
            case "integer32":
                var buf = new Buffer(4);
                buf.writeInt32BE(value.value, 0);
                familyMap.value = buf;
                break;
            case "float":
                var buf = new Buffer(4);
                buf.writeFloatBE(value.value, 0);
                familyMap.value = buf;
                break;
            case "number":
            case "integer48":
                var buf = new Buffer(8);
                buf.writeIntBE(value.value, 2, 6);
                familyMap.value = buf;
                break;
            case "UInteger48":
                var buf = new Buffer(6);
                buf.writeUIntBE(value.value, 0);
                familyMap.value = buf;
                break;
            case "long":
            case "int64":
                familyMap.value = value.value.toBuffer(true);
            default:
                throw new Error('Unsupported value.type for put qualifier ' + family + ':' + qualifier + ' , Value: ' + JSON.stringify(value));
        }
    } else {
        familyMap.value = value.toString();
    }

    if (timestamp) {
        familyMap.timestamp = new Int64(timestamp);
    }
    this.columns.push(familyMap);
    return this;
};


module.exports = Put;