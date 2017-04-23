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

const debug = require('debug')('Node-Thrift2-HBase');

var Service = function (options) {
    this.clientPool = ClientPool(options);
    this.hosts = options.hosts;
    this.saltMap = options.saltMap || {};
    let cachedTables = options.cachedTables || [];
    this.cachedTablesSet = new Set(cachedTables);
    this.cache = new Cache(_.bind(this.fetchRowAsync, this), options);
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


Service.prototype.fetchRow = function (table, key, columns, options, callback) {
    debug('fetching from HBase');
    var hbasePool = this.clientPool;
    var args = arguments;
    var _callback = args[args.length - 1];
    this.clientPool.acquire(function (err, hbaseClient) {
        if (err)
            return _callback(err);

        function releaseAndCallback(err, data) { //get users table
            if (err) {
                //destroy client on error
                hbasePool.destroy(hbaseClient);
                return _callback(err);
            }
            //release client in the end of use.
            hbasePool.release(hbaseClient);
            return _callback(null, data);
        }

        args[args.length - 1] = releaseAndCallback;
        hbaseClient.getRow.apply(hbaseClient, args);
    });
};

Service.prototype.Get = Get;
Service.prototype.get = function(table, get, callback) {
    var hbasePool = this.clientPool;

    this.clientPool.acquire(function (err, hbaseClient) {
        if (err)
            return callback(err);

        hbaseClient.get(table, get, function releaseAndCallback(err, data) {
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
Service.prototype.getRow = function (table, key, columns, options, callback) {
    key = this.salt(table, key);
    var self = this;
    var cache = this.cache;
    debug('getting row', key, 'from table', table, 'with columns', columns);

    var args = arguments;
    args[1] = key;
    var _callback = args[args.length - 1];
    if ((options && options.cacheQuery) || this.cachedTablesSet.has(table)) {
        cache.getRow(table, key, columns, options)
            .then(function (cachedGet) {
                return _callback(null, cachedGet);
            })
            .catch(_callback);
    } else {
        this.fetchRow.apply(this, args);
    }
};

Service.prototype.Put = Put;
Service.prototype.put = function (table, put, callback) {
    var hbasePool = this.clientPool;

    this.clientPool.acquire(function (err, hbaseClient) {
        if (err)
            return callback(err);

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
    var hbasePool = this.clientPool;

    this.clientPool.acquire(function (err, hbaseClient) {
        if (err)
            return callback(err);

        hbaseClient.scan(table, scan,
            function releaseAndCallback(err, data) {
                if (err) {
                    //destroy client on error
                    hbasePool.destroy(hbaseClient);
                    return callback(err);
                }
                //release client in the end of use.
                hbasePool.release(hbaseClient);
                return callback(null, data);
            })
    });
};

Service.prototype.Del = Del;
Service.prototype.del = function(table, del, callback) {
    var hbasePool = this.clientPool;

    this.clientPool.acquire(function (err, hbaseClient) {
        if (err)
            return callback(err);

        hbaseClient.del(table, del, function releaseAndCallback(err, data) {
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

Service.prototype.Inc = Inc;
Service.prototype.inc = function(table, inc, callback) {
    var hbasePool = this.clientPool;

    this.clientPool.acquire(function (err, hbaseClient) {
        if (err)
            return callback(err);

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

Promise.promisifyAll(Service.prototype);

module.exports = Service.create;