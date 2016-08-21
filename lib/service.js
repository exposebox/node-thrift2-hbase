'use strict';
var debug = require('debug')('Node-Thrift2-HBase');

var ClientPool = require('./client');
var Promise = require('bluebird');

var Service = function (options) {
    this.clientPool = ClientPool(options);
    this.hosts = options.hosts;
    this.saltMap = options.saltMap || {};
};

Service.create = function (options) {
    return new Service(options);
};

function noop(k) {
    return k;
}

function saltByLastKeyCharCode(key) {
    let charCode = key.codePointAt(key.length - 1);
    let salt = charCode % 10;
    return salt.toString() + key;
}

Service.prototype.saltFunctions = {
    saltByLastKeyCharCode: saltByLastKeyCharCode
};

Service.prototype.salt = function (table, key) {
    debug('salting key', key, 'for table', table);
    return (this.saltMap[table] || noop)(key);
};

Service.prototype.getRow = function (table, key, columns, options, callback) {
    var hbasePool = this.clientPool;
    key = this.salt(table, key);
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

Service.prototype.scan = function (table, options, callback) {
    var hbasePool = this.clientPool;

    this.clientPool.acquire(function (err, hbaseClient) {
        if (err)
            return callback(err);

        var scan = hbaseClient.Scan();
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

Promise.promisifyAll(Service.prototype)
module.exports = Service.create;