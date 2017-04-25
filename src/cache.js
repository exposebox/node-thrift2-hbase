'use strict';

const _ = require('underscore');
const ms = require('ms');
const Cache = require('caching-map');
const Promise = require('bluebird');

const debug = require('debug')('node-thrift2-hbase:cache');

class HBaseThriftClientCache extends Cache {

    constructor(fetchfunction, options) {
        super((options && options.limit ) || 500000);

        this.fetch = fetchfunction;
        this.ttl = (options && options.ttl) || ms('5m');
        this.materialize = function (getObj) {
            debug('materializing', getObj);
            return fetchfunction(getObj.table, getObj);
        }
    }

    get(table, getObj, options, callback) {
        getObj.table = table;
        debug(getObj);

        super.get(getObj)
            .then(value => callback(null, Promise.resolve(value)))
            .catch(err => callback(err));
    }

}

Promise.promisifyAll(HBaseThriftClientCache.prototype);

module.exports = HBaseThriftClientCache;