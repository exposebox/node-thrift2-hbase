'use strict';

const _ = require('underscore');
const ms = require('ms');
const Cache = require('caching-map');
const Promise = require('bluebird');

const debug = require('debug')('HBase-Thrift-Client-Cache');

class HBaseThriftClientCache extends Cache {

    constructor(fetchfunction, options) {
        super((options && options.limit ) || Infinity);

        this.fetch = fetchfunction;
        this.ttl = (options && options.ttl) || ms('5m');
    }

    getRow(table, key, columns, options) {
        let self = this;
        let prefix = [table, key, JSON.stringify(options)].join('#');

        let columnValues = _.chain(columns)
            .map(function (col) {
                return prefix + '#' + col;
            })
            .map(function (cacheKey) {
                debug('getting value for key', cacheKey, 'from cache');
                let result = self.get(cacheKey);
                !result && debug('no value found in cache for key', cacheKey);
                return result;
            })
            .value(); //in case of whole column families

        if (_.every(columnValues, function (colValue) {
                return colValue !== undefined;
            })) {
            debug('all requested columns found in cache!');
            return Promise.all(columnValues).then(function (columnValues) {
                columnValues = _.chain(columnValues).compact().flatten().value();
                return {
                    row: !_.isEmpty(columnValues) ? key : null,
                    columnValues,
                    foundInCache: true
                };
            });
        }

        return self.materializeAndCacheRow(table, key, columns, options);
    }

    materializeAndCacheRow(table, key, columns, options) {
        let self = this;
        let prefix = [table, key, JSON.stringify(options)].join('#');
        let fetchedData = this.fetch(table, key, columns, options);
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

        function setValuePromise(key) {
            self.set(key,
                indexedFetchedData.then(function (columnValues) {
                    debug('resolving data value in cache for key', key);
                    return columnValues[key];
                }),
                {
                    ttl: (options && options.ttl) || self.ttl
                });
        }

        _.each(columns, function (col) {
            let cacheKey = prefix + '#' + col;
            if (self.isWholeColumnFamily(col)) {
                debug('get for a whole column family!', col);
                self.set(cacheKey,
                    indexedFetchedData.then(function (columnValues) {
                        debug('columnValues for cf caching:', columnValues);
                        let cachedResult = _.chain(columnValues)
                            .keys()
                            .filter(function (columnKey) {
                                debug('filtering columnKey', columnKey);
                                return columnKey.indexOf(cacheKey) == 0;
                            })
                            .each(function (colKey) {
                                debug('caching individual column', colKey);
                                setValuePromise(colKey);
                            })
                            .map(function (colKey) {
                                debug('extracting column key value', colKey, columnValues[colKey]);
                                return columnValues[colKey];
                            })
                            .value();
                        debug('caching whole column family', cachedResult);
                        return cachedResult;
                    }));
            }
            else {
                setValuePromise(cacheKey);
            }
        });

        return fetchedData;
    }

    isWholeColumnFamily(column) {
        return column.indexOf(':') < 0;
    }
}

Promise.promisifyAll(HBaseThriftClientCache.prototype);

module.exports = HBaseThriftClientCache;