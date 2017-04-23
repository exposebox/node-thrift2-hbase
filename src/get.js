"use strict";

var Int64 = require('node-int64');

class Get {
    constructor(row) {
        this.row = row;
        this.maxVersions = 1;
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

    setMaxVersions(maxVersions) {
        if (maxVersions <= 0) {
            maxVersions = 1;
        }
        this.maxVersions = maxVersions;
        return this;
    }

    setTimeRange(timeRange) {
        this.timeRange = timeRange;
    }
}

module.exports = Get;