"use strict";

function Inc(row) {
    if (!(this instanceof Inc)) {
        return new Inc(row);
    }
    this.row = row;
    this.familyList = [];
}

Inc.prototype.add = function (family, qualifier, amount) {
    var familyMap = {};
    familyMap.family = family;
    familyMap.qualifier = qualifier;
    familyMap.amount = (amount === 0) ? 0 : (amount || 1);
    this.familyList.push(familyMap);
    return this;
};


module.exports = Inc;