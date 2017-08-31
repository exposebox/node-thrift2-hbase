'use strict';
const _ = require('underscore');
const Promise = require('bluebird');

const ClientPool = require('./client');
const Cache = require('./cache');
var Get = require('./get');
var Put = require('./put');
var Del = require('./del');
var Inc = require('./inc');
var Scan = require('./scan');

const debug = require('debug')('node-thrift2-hbase:service');

var Service = function (options) {
    this.clientPool = ClientPool(options);
    this.hosts = options.hosts;
    this.saltMap = options.saltMap || {};
    let cachedTables = options.cachedTables || [];
    this.cachedTablesSet = new Set(cachedTables);
    this.cache = new Cache(
        _.bind(this.applyGetOnClientAsync, this),
        options.cacheOptions);
};

Service.create = function (options) {
    return new Service(options);
};

function noop(k) {
    return k;
}

function saltByLastKeyCharCode(key) {
    var charCode = key.codePointAt(key.length - 1);
    var salt = charCode % 10;
    var salted = salt.toString() + key;
    debug('salting key', key, '=>', salted);
    return salted;
}

Service.prototype.saltFunctions = {
    saltByLastKeyCharCode: saltByLastKeyCharCode
};

Service.prototype.salt = function (table, key) {
    return (this.saltMap[table] || noop)(key);
};

Service.prototype.applyActionOnClient = function (actionName, table, queryObj, callback) {
    debug('applyActionOnClient: applying action', queryObj);
    var hbasePool = this.clientPool;
    this.clientPool.acquire(function (err, hbaseClient) {
        if (err)
            return callback(err);

        function releaseAndCallback(err, data) {
            if (err) {
                //destroy client on error
                hbasePool.destroy(hbaseClient);
                return callback(err);
            }
            //release client in the end of use.
            hbasePool.release(hbaseClient);
            return callback(null, data);
        }

        hbaseClient[actionName](table, queryObj, releaseAndCallback);
    });
};

Service.prototype.applyGetOnClient = function (table, queryObj, callback) {
    this.applyActionOnClient('get', table, queryObj, callback);
}
Service.prototype.Get = Get;
Service.prototype.get = function (table, get, options, callback) {
    if (callback == null) {
        callback = options;
        options = {};
    }

    get.row = this.salt(table, get.row);
    var cache = this.cache;
    debug('getting from table', table);
    debug(get);

    if ((options && options.cacheQuery) || this.cachedTablesSet.has(table)) {
        cache.get(table, get, options, callback)
    } else {
        this.applyActionOnClient('get', table, get, callback);
    }
};
Service.prototype.getRow = function (table, key, columns, options, callback) {
    debug('getting row', key, 'from table', table, 'with columns', columns);
    const getObj = new Get(key, options);

    if (columns && columns.length > 0) {
        _.each(columns, function (ele, idx) {
            if (ele.indexOf(':') != -1) {
                const cols = ele.split(':');
                const family = cols[0];
                const qualifier = cols[1];
                getObj.addColumn(family, qualifier);
            } else {
                getObj.addFamily(ele);
            }
        });
    }

    this.get(table, getObj, options, callback);
};

Service.prototype.Put = Put;
Service.prototype.put = function (table, put, callback) {
    this.applyActionOnClient('put', table, put, callback);
};
Service.prototype.putRow = function (table, key, cf, valuesMap, callback) {
    var hbasePool = this.clientPool;
    key = this.salt(table, key);

    this.clientPool.acquire(function (err, hbaseClient) {
        if (err)
            return callback(err);

        var put = hbaseClient.Put(key);
        for (var col in valuesMap) {
            var value = valuesMap[col];
            if (value !== undefined && value !== null)
                put.add(cf, col, value);
        }
        hbaseClient.put(table, put, function releaseAndCallback(err, data) {
            if (err) {
                //destroy client on error
                hbasePool.destroy(hbaseClient);
                return callback(err);
            }
            //release client in the end of use.
            hbasePool.release(hbaseClient);
            return callback(null, data);
        });
    });
};

//cellAmounts = [{cf:f,qualifier:q,amount:1}, ...]
Service.prototype.incRow = function (table, key, cellAmounts, callback) {
    var hbasePool = this.clientPool;
    key = this.salt(table, key);

    this.clientPool.acquire(function (err, hbaseClient) {
        if (err)
            return callback(err);

        var inc = hbaseClient.Inc(key);
        for (var cellIndx in cellAmounts) {
            var incCell = cellAmounts[cellIndx];
            if (incCell.cf && incCell.qualifier)
                inc.add(incCell.cf, incCell.qualifier, incCell.amount);
            else
                return callback(new Error("CellAmount must be in the form of {cf:\"f\",qualifier:\"q\",amount:\"1\""));
        }
        hbaseClient.inc(table, inc, function releaseAndCallback(err, data) {
            if (err) {
                //destroy client on error
                hbasePool.destroy(hbaseClient);
                return callback(err);
            }
            //release client in the end of use.
            hbasePool.release(hbaseClient);
            return callback(null, data);
        });
    });
};

Service.prototype.Scan = Scan;
Service.prototype.scan = function (table, scan, callback) {
    this.applyActionOnClient('scan', table, scan, callback);
};

Service.prototype.Del = Del;
Service.prototype.del = function (table, del, callback) {
    this.applyActionOnClient('del', table, del, callback);
};

Service.prototype.Inc = Inc;
Service.prototype.inc = function (table, inc, callback) {
    this.applyActionOnClient('inc', table, inc, callback);
};

Promise.promisifyAll(Service.prototype);

module.exports = Service.create;