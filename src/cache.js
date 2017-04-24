'use strict';

const _ = require('underscore');
const ms = require('ms');
const Cache = require('caching-map');
const Promise = require('bluebird');

const debug = require('debug')('node-thrift2-hbase:HBase-Thrift-Client-Cache');

class HBaseThriftClientCache extends Cache {

    constructor(fetchfunction, options) {
        super((options && options.limit ) || 500000);

        this.fetch = fetchfunction;
        this.ttl = (options && options.ttl) || ms('5m');
    }

    get(table, getObj, options, callback) {
        getObj.table = table;

        super.get(getObj)
            .then(value => callback(null, value))
            .catch(err => callback(err));
    }

    materialize(getObj) {
        return this.fetch(getObj.table, getObj);
    }
}

Promise.promisifyAll(HBaseThriftClientCache.prototype);

module.exports = HBaseThriftClientCache;