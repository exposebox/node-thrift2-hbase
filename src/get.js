"use strict";

var Int64 = require('node-int64');

class Get {
    constructor(row,options) {
        this.row = row;
        this.setMaxVersions(options && options.maxVersions);
        this.setTimeRange(options&&options.timeRange);
        this.columns = [];
    }

    add(family, qualifier, timestamp) {
        var familyMap = {};
        familyMap.family = family;
        if (qualifier) {
            familyMap.qualifier = qualifier;
        }
        if (timestamp) {
            familyMap.timestamp = new Int64(timestamp);
        }
        this.columns.push(familyMap);
        return this;
    }

    addFamily(family) {
        var familyMap = {};
        familyMap.family = family;
        this.columns.push(familyMap);
        return this;
    }

    addColumn(family, qualifier) {
        var familyMap = {};
        familyMap.family = family;
        familyMap.qualifier = qualifier;
        this.columns.push(familyMap);
        return this;
    }

    addTimestamp(family, qualifier, timestamp) {
        var familyMap = {};
        familyMap.family = family;
        familyMap.qualifier = qualifier;
        familyMap.timestamp = new Int64(timestamp);
        this.columns.push(familyMap);
        return this;
    }

    // default to 1 for performance, HBase default is 3
    setMaxVersions(maxVersions) {
        if (!maxVersions||maxVersions <= 0) {
            maxVersions = 1;
        }
        this.maxVersions = maxVersions;
        return this;
    }

    setTimeRange(timeRange) {
        this.timeRange = timeRange;
        return this;
    }
}

module.exports = Get;