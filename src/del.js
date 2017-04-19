"use strict";


function Del(row) {
    if (!(this instanceof Del)) {
        return new Del(row);
    }
    this.row = row;
    this.familyList = [];
}

Del.prototype.add = function (family, qualifier, timestamp) {
    var familyMap = {};
    familyMap.family = family;
    if(qualifier){
        familyMap.qualifier = qualifier;
    }
    if(timestamp){
        familyMap.timestamp = timestamp;
    }
    this.familyList.push(familyMap);
    return this;
};

Del.prototype.addFamily = function (family) {
    var familyMap = {};
    familyMap.family = family;
    this.familyList.push(familyMap);
    return this;
};

Del.prototype.addColumn = function (family, qualifier) {
    var familyMap = {};
    familyMap.family = family;
    familyMap.qualifier = qualifier;
    this.familyList.push(familyMap);
    return this;
};

Del.prototype.addTimestamp = function (family, qualifier, timestamp) {
    var familyMap = {};
    familyMap.family = family;
    familyMap.qualifier = qualifier;
    familyMap.timestamp = timestamp;
    this.familyList.push(familyMap);
    return this;
};



module.exports = Del;