"use strict";

var Int64 = require('node-int64');
const serde = require('./serde');

class Put {
    constructor(row) {
        this.row = row;
        this.columns = [];
    }

    add(family, qualifier, value, timestamp) {
        var familyMap = {};
        familyMap.family = family;
        familyMap.qualifier = qualifier;
        familyMap.value = serde.serialize(value);

        if (timestamp) {
            familyMap.timestamp = new Int64(timestamp);
        }

        this.columns.push(familyMap);
        return this;
    }

    addObject(family, object, timestamp) {
        for (const prop in object) {
            const value = object[prop];
            if (value !== undefined && value !== null)
                put.add(cf, prop, value, timestamp);
        }
    }
}


module.exports = Put;