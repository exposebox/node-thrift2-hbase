'use strict';

const _ = require('underscore');
const Int64 = require('node-int64');

const Get = require('./get');
const Put = require('./put');
const Del = require('./del');
const Inc = require('./inc');
const Scan = require('./scan');
const thrift = require('thrift');
const HBase = require('../gen-nodejs/THBaseService');
const HBaseTypes = require('../gen-nodejs/hbase_types');
const poolModule = require('generic-pool');

const debug = require('debug')('node-thrift-hbase:client');

const createClientPool = function (options) {
    const hostsHistory = {};

    options.hosts.forEach(function (host) {
        const hostHistory = {
            host: host,
            errors: 0,
            lastErrorTime: 0
        };
        hostsHistory[host] = hostHistory;
    });

    const halfLifeErrorsInterval = setInterval(function halfLifeErrors() {
        _.forEach(hostsHistory, function (hostHistory) {
            hostHistory.errors = Math.floor(hostHistory.errors / 2);
        });
    }, 60 * 1000);

    const markHostError = (host) => {
        const hostHistory = hostsHistory[host];

        if (hostHistory) {
            hostHistory.lastErrorTime = Date.now();
            hostHistory.errors += 1;
            hostsHistory[host] = hostHistory;
        }
    };

    const pool =
        poolModule.Pool({
            name: 'hbase',
            create: function (callback) {
                let isCallbackCalled = false;

                function callbackWrapper(error, client) {
                    if (isCallbackCalled)
                        return;

                    if (error) {
                        client._invalid = true;
                        markHostError(client.host);
                    }

                    isCallbackCalled = true;
                    callback(error, client);
                }


                if (!options.hosts || options.hosts.length < 1) {
                    return callback(new Error('hosts is empty'));
                }


                //filter hostsHistory with connect error.
                const hostsToSelect = _.values(hostsHistory).filter(function (hostHistory) {
                    return !hostHistory.errors ||
                        ((Date.now() - hostHistory.lastErrorTime) > Math.pow(2, hostHistory.errors));
                });

                if (hostsToSelect.length < 1)
                    return callback(new Error('All host appear to be down'));

                //select one host from list randomly
                const host = hostsToSelect[Math.floor(Math.random() *
                    hostsToSelect.length)].host;

                const clientOption = {
                    port: options.port,
                    host: host,
                    timeout: options.timeout
                };
                const client = new Client(clientOption);


                client.connection.on('connect', function () {
                    client.client = thrift.createClient(HBase, client.connection);
                    callbackWrapper(null, client);
                });

                //todo: 1. Need to retry with different host. 2. Add cool time for host with errors.
                client.connection.on('error', function (err) {
                    debug('Thrift connection error', err, client.host);
                    callbackWrapper(err, client);
                });

                client.connection.on('close', function () {
                    debug('Thrift close connection error', client.host);
                    callbackWrapper(new Error('Thrift close connection'), client);
                });

                client.connection.on('timeout', function () {
                    debug('Thrift timeout connection error', client.host);
                    callbackWrapper(new Error('Thrift timeout connection'), client);
                });

            },
            validate: function (client) {
                return !client._invalid;
            },
            destroy: function (client) {
                client.connection.end();
                //try to disconnect from child process gracefully
                client.child && client.child.disconnect();
            },
            min: options.minConnections || 0,
            max: options.maxConnections || 10,
            idleTimeoutMillis: options.idleTimeoutMillis || 5000
        });

    pool.drain = _.wrap(pool.drain, wrapped => {
        clearInterval(halfLifeErrorsInterval);

        wrapped.call(pool);
    });

    return pool;
};

const Client = function (options) {
    if (!options.host || !options.port) {
        throw new Error('host or port is none');
    }
    this.host = options.host || 'master';
    this.port = options.port || '9090';

    const connection = thrift.createConnection(this.host, this.port, {connect_timeout: options.timeout || 0});
    connection.connection.setKeepAlive(true);
    this.connection = connection;
};

Client.create = function (options) {
    return new Client(options);
};

Client.prototype.Put = function (row) {
    return new Put(row);
};

Client.prototype.Del = function (row) {
    return new Del(row);
};

Client.prototype.Inc = function (row) {
    return new Inc(row);
};

Client.prototype.scan = function (table, scan, callback) {
    const tScan = new HBaseTypes.TScan(scan);

    this.client.getScannerResults(table, tScan, scan.numRows, function (serr, data) {
        if (serr) {
            callback(serr.message.slice(0, 120));
        } else {
            callback(null, scan.objectsFromData(data));
        }
    });
};

Client.prototype.get = function (table, getObj, callback) {
    const tGet = new HBaseTypes.TGet(getObj);
    this.client.get(table, tGet, function (err, data) {

        if (err) {
            callback(err.message.slice(0, 120));
        } else {
            callback(null, getObj.objectFromData(data));
        }

    });
};

Client.prototype.put = function (table, param, callback) {
    const row = param.row;
    if (!row) {
        callback(null, 'rowKey is null');
    }
    const query = {};

    query.row = row;
    const qcolumns = [];
    if (param.columns && param.columns.length > 0) {
        _.each(param.columns, function (ele, idx) {
            qcolumns.push(new HBaseTypes.TColumnValue(ele));
        });
        query.columnValues = qcolumns;
    }

    const tPut = new HBaseTypes.TPut(query);

    this.client.put(table, tPut, function (err) {

        if (err) {
            callback(err);
        } else {
            callback(null);
        }
    });
};

Client.prototype.putRow = function (table, row, columns, value, timestamp, callback) {
    const args = arguments;
    const query = {};

    if (args.length <= 0) {
        throw new Error('Expected 5 arguments got 0');
    }

    callback = args[args.length - 1];

    if (callback && typeof callback !== 'function') {
        throw new Error('callback is not a function');
    }

    if (args.length < 5) {
        callback(new Error('arguments arg short of 5'));
        return;
    }

    if (args.length >= 5) {
        if (args[2].indexOf(':') === -1) {
            callback(new Error('family and qualifier must have it,example ["info:name"]'));
            return;
        }
    }


    query.row = row;
    const qcolumns = [];
    if (columns) {
        let cols = [], temp = {};
        cols = columns.split(':');
        temp = {
            family: cols[0],
            qualifier: cols[1],
            value: value
        };
        if (timestamp) {
            temp.timestamp = new Int64(timestamp);
        }
        qcolumns.push(new HBaseTypes.TColumnValue(temp));
        query.columnValues = qcolumns;
    }

    const tPut = new HBaseTypes.TPut(query);

    this.client.put(table, tPut, function (err) {

        if (err) {
            callback(err);
        } else {
            callback(null);
        }
    });
};

Client.prototype.del = function (table, param, callback) {
    const tDelete = new HBaseTypes.TDelete(param);
    this.client.deleteSingle(table, tDelete, function (err) {

        if (err) {
            callback(err);
        } else {
            callback(null);
        }
    });
};

Client.prototype.delRow = function (table, row, columns, timestamp, callback) {
    const args = arguments;
    const query = {};

    if (args.length <= 0) {
        throw new Error('Expected 3 arguments got 0');
    }

    callback = args[args.length - 1];

    if (callback && typeof callback !== 'function') {
        throw new Error('callback is not a function');
    }

    if (args.length < 3) {
        callback(new Error('arguments arg short of 3'));
        return;
    }

    if (args.length === 5) {
        if (args[2].indexOf(':') === -1) {
            callback(new Error('family and qualifier must have it,example ["info:name"]'));
            return;
        }
    }


    query.row = row;
    const qcolumns = [];
    if (args.length >= 4 && columns) {
        let cols = [], temp = {};
        if (columns.indexOf(':') != -1) {
            cols = columns.split(':');
            temp = {
                family: cols[0],
                qualifier: cols[1]
            };
            if (args.length === 5) {
                temp.timestamp = timestamp;
            }
        } else {
            temp = {
                family: columns
            }
        }

        qcolumns.push(new HBaseTypes.TColumn(temp));
        query.columns = qcolumns;
    }

    const tDelete = new HBaseTypes.TDelete(query);

    this.client.deleteSingle(table, tDelete, function (err, data) {
        if (err) {
            callback(err);
        } else {
            callback(null, data);
        }
    });
};

Client.prototype.inc = function (table, param, callback) {
    const row = param.row;
    if (!row) {
        callback(new Error('rowKey is null'));
    }
    const query = {};

    query.row = row;
    const qcolumns = [];
    if (param.columns && param.columns.length > 0) {
        _.each(param.columns, function (ele, idx) {
            qcolumns.push(new HBaseTypes.TColumnIncrement(ele));
        });
        query.columns = qcolumns;
    }


    const tIncrement = new HBaseTypes.TIncrement(query);

    this.client.increment(table, tIncrement, function (err, data) {

        if (err) {
            callback(err);
        } else {
            callback(null, data);
        }
    });

};

Client.prototype.incRow = function (table, row, columns, callback) {
    const args = arguments;
    const query = {};

    if (args.length <= 0) {
        throw new Error('Expected 3 arguments got 0');
    }

    callback = args[args.length - 1];

    if (callback && typeof callback !== 'function') {
        throw new Error('callback is not a function');
    }

    if (args.length < 3) {
        callback(new Error('arguments arg short of 3'));
        return;
    }

    if (args.length >= 3) {
        if (args[2].indexOf(':') === -1) {
            callback(new Error('family and qualifier must have it,example ["info:counter"]'));
            return;
        }
    }

    query.row = row;
    const qcolumns = [];
    if (columns) {
        let cols = [], temp = {};
        cols = columns.split(':');
        temp = {
            family: cols[0],
            qualifier: cols[1]
        };
        qcolumns.push(new HBaseTypes.TColumn(temp));
        query.columns = qcolumns;
    }

    const tIncrement = new HBaseTypes.TIncrement(query);

    this.client.increment(table, tIncrement, function (err, data) {
        if (err) {
            callback(err);
        } else {
            callback(null, data);
        }
    });
};

module.exports = createClientPool;