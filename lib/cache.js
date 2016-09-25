'use strict';

const _ = require('underscore');
const ms = require('ms');
const Cache = require('caching-map');
const Promise = require('bluebird');

const debug = require('debug')('HBase-Thrift-Client-Cache');

class HBaseThriftClientCache extends Cache {

    constructor(hbaseClientPool, options) {
        super((options && options.limit ) || Infinity);

        this.clientPool = hbaseClientPool;
        this.ttl = ms((options && options.ttl) || ms('5m'));
    }

    getRow(table, key, columns, options) {
        let self = this;
        let prefix = [table, key, JSON.stringify(options)].join('#');

        let columnValues = _(columns)
            .map(function (col) {
                return prefix + '#' + col;
            })
            .map(function (key) {
                debug('getting value for key', key, 'from cache');
                let result = self.get(key);
                !result && debug('no value found in cache for key', key);
                return result;
            });

        if (_.every(columnValues, function (colValue) {
                return colValue != undefined;
            })) {
            debug('all requested columns found in cache!');
            return Promise.all(columnValues).then(function (columnValues) {
                columnValues = _.compact(columnValues);
                return {
                    row: key,
                    columnValues
                };
            });
        }

        return self.materializeAndCacheRow(table, key, columns, options);
    }

    materializeAndCacheRow(table, key, columns, options) {
        let self = this;
        let prefix = [table, key, JSON.stringify(options)].join('#');
        let fetchedData = this.fetchAsync(table, key, columns, options);
        debug('materializing row', prefix);
        let indexedFetchedData = fetchedData.then(function (data) {
            debug('indexing fetched data', prefix);
            return _.indexBy(data.columnValues, function (colValue) {
                let family = colValue.family.toString();
                let qualifier = colValue.qualifier.toString();
                let column = family + ':' + qualifier;
                return prefix + '#' + column;
            });
        });

        _.each(columns, function (col) {
            let key = prefix + '#' + col;
            self.set(key,
                indexedFetchedData.then(function (columnValuesByKey) {
                    debug('resolving data value in cache for key', key);
                    return columnValuesByKey[key];
                }));
        });

        return fetchedData;
    }

    fetch(table, key, columns, options, callback) {
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
    }
}

Promise.promisifyAll(HBaseThriftClientCache.prototype);

module.exports = HBaseThriftClientCache;