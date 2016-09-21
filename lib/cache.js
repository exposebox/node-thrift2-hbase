'use strict';

const _ = require('underscore');
const ms = require('ms');
const Cache = require('caching-map');
const Promise = require('bluebird');

const debug = require('debug')('HBase-Thrift-Client-Cache');

class HBaseThriftClientCache extends Cache {

    constructor(ttl, limit) {
        super(limit || Infinity);

        this.ttl = ms(ttl || ms('5m'));
    }

    getRow(table, key, columns, options) {
        let self = this;
        let prefix = [table, key, JSON.stringify(options)].join('#');

        let keysByColumns = Promise
            .map(columns, function (col) {
                return prefix + '#' + col;
            })
            .map(function (key) {
                debug('getting value for key', key, 'from cache');
                return new Promise(function (resolve, reject) {
                    self.get(key) || Promise.reject('key ' + key + ' not found in cache!');
                });
            })
            .all()
            .catch(function (err) {
                debug(err);
            });

        return keysByColumns
            .then(function (columnValues) {
                debug('all requested columns found in cache!');
                return {
                    columnValues
                }
            })
    }

    setRow(table, key, columns, options, dataPromise) {
        let self = this;
        let prefix = [table, key, JSON.stringify(options)].join('#');

        let cacheReceivedData = dataPromise.then(function (data) {
            _.each(data.columnValues, function (colValue) {
                let family = colValue.family.toString();
                let qualifier = colValue.qualifier.toString();
                let colValueKey = prefix + '#' + family + ':' + qualifier;

                self.set(colValueKey, colValue);
            });
        });

        _(columns)
            .map(function (col) {
                return prefix + '#' + col;
            })
            .each(function (key) {
                this.set(key, cacheReceivedData.then(function () {
                    return self.get(key);
                }));
            });
    }

}

module.exports = HBaseThriftClientCache;