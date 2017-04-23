"use strict";

function Inc(row) {
    if (!(this instanceof Inc)) {
        return new Inc(row);
    }
    this.row = row;
    this.columns = [];
}

Inc.prototype.add = function (family, qualifier, amount) {
    var familyMap = {};
    familyMap.family = family;
    familyMap.qualifier = qualifier;
    familyMap.amount = (amount === 0) ? 0 : (amount || 1);
    this.columns.push(familyMap);
    return this;
};


module.exports = Inc;