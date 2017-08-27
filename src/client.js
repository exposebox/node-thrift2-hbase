'use strict';

var _ = require('underscore');
var Int64 = require('node-int64');

var Get = require('./get');
var Put = require('./put');
var Del = require('./del');
var Inc = require('./inc');
var Scan = require('./scan');
var thrift = require('thrift');
var HBase = require('../gen-nodejs/THBaseService');
var HBaseTypes = require('../gen-nodejs/hbase_types');
var poolModule = require('generic-pool');

var ClientPool = function (options) {
    var PoolId = (Math.random() * 100000000).toString(36);

    var hostsHistory = {};
    options.hosts.forEach(function (host) {
        var hostHistory = {
            host: host,
            errors: 0,
            lastErrorTime: 0
        };
        hostsHistory[host] = hostHistory;
    });

    setInterval(function halflifeErrors() {
        _.forEach(hostsHistory, function (hostHistory) {
            hostHistory.errors = Math.floor(hostHistory.errors / 2);
        });
    }, 60 * 1000);

    function markHostError(host) {
        var hostHistory = hostsHistory[host];
        if (hostHistory) {
            hostHistory.lastErrorTime = Date.now();
            hostHistory.errors += 1;
            hostsHistory[host] = hostHistory;
        }
    }


    var pool = poolModule.Pool({
        name: 'hbase',
        create: function (callback) {

            var isCallbackCalled = false;

            function callbackWrapper(error, client) {
                if (error) {
                    client._invalid = true;
                    markHostError(client.host);
                }

                if (isCallbackCalled)
                    return;

                isCallbackCalled = true;
                callback(error, client);
            }


            if (!options.hosts || options.hosts.length < 1) {
                return callback(new Error('hosts is empty'));
            }


            //filter hostsHistory with connect error.
            var hostsToSelect = _.values(hostsHistory).filter(function (hostHistory) {
                return !hostHistory.errors ||
                    ((Date.now() - hostHistory.lastErrorTime) > Math.pow(2, hostHistory.errors));
            });

            if (hostsToSelect.length < 1)
                return callback(new Error('All host appear to be down'));

            //select one host from list randomly
            var host = hostsToSelect[Math.floor(Math.random() *
                hostsToSelect.length)].host;

            var clientOption = {
                port: options.port,
                host: host,
                timeout: options.timeout
            };
            var client = new Client(clientOption);


            client.connection.on('connect', function () {
                client.client = thrift.createClient(HBase, client.connection);
                callbackWrapper(null, client);
            });

            //todo: 1. Need to retry with different host. 2. Add cool time for host with errors.
            client.connection.on('error', function (err) {
                console.log('Thrift connection error', err, client.host);
                callbackWrapper(err, client);
            });

            client.connection.on('close', function () {
                console.log('Thrift close connection error', client.host);
                callbackWrapper(new Error('Thrift close connection'), client);
            });

            client.connection.on('timeout', function () {
                console.log('Thrift timeout connection error', client.host);
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
            ////kill child if disconnect didn't work
            //var killTimeout = setTimeout(function(){
            //    client.child.kill();
            //},5000);
            //
            ////cancel kill timeout on disconnect
            //client.child.on('disconnect',function(){
            //    clearTimeout(killTimeout);
            //});

        },
        min: options.minConnections || 0,
        max: options.maxConnections || 10,
        idleTimeoutMillis: options.idleTimeoutMillis || 3600000
    });

    return pool;
};


var Client = function (options) {
    if (!options.host || !options.port) {
        throw new Error('host or port is none');
    }
    this.host = options.host || 'master';
    this.port = options.port || '9090';

    var connection = thrift.createConnection(this.host, this.port, {connect_timeout: options.timeout || 0});
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
    var tScan = new HBaseTypes.TScan(scan);
    var that = this;
    this.client.getScannerResults(table, tScan, scan.numRows, function (serr, data) {
        if (serr) {
            callback(serr.message.slice(0, 120));
        } else {
            callback(null, scan.objectsFromData(data));
        }
    });
};
Client.prototype.scanLoopFetchHelp = function (that, ret, scannerId, numRows, callback) {
    that.client.getScannerRows(scannerId, numRows, function (serr, data) {
        if (serr) {
            that.client.closeScanner(scannerId, function (err) {
                if (err) {
                    console.log(err);
                }
            });
            callback(serr.message.slice(0, 120));
            return;
        } else {
            if (data.length > 0) {
                Array.prototype.push.apply(ret, data);
                that.scanLoopFetchHelp(that, ret, scannerId, numRows, callback);
            } else {
                that.client.closeScanner(scannerId, function (err) {
                    if (err) {
                        console.log(err);
                    }
                });
                callback(null, ret);
            }
        }
    });
};
Client.prototype.scanLoopFetch = function (table, scan, callback) {
    var tScan = new HBaseTypes.TScan(scan);
    var that = this;
    this.client.openScanner(table, tScan, function (err, scannerId) {
        if (err) {
            that.client.closeScanner(scannerId, function (err) {
                if (err) {
                    console.log(err);
                }
            });
            callback(err.message.slice(0, 120));
            return;
        } else {
            that.scanLoopFetchHelp(that, [], scannerId, scan.numRows, function (serr, data) {
                callback(serr, scan.objectsFromData(data));
            });
        }
    });
};

Client.prototype.get = function (table, getObj, callback) {
    var tGet = new HBaseTypes.TGet(getObj);
    this.client.get(table, tGet, function (err, data) {

        if (err) {
            callback(err.message.slice(0, 120));
        } else {
            callback(null, getObj.objectFromData(data));
        }

    });
};

Client.prototype.put = function (table, param, callback) {
    var row = param.row;
    if (!row) {
        callback(null, 'rowKey is null');
    }
    var query = {};

    query.row = row;
    var qcolumns = [];
    if (param.columns && param.columns.length > 0) {
        _.each(param.columns, function (ele, idx) {
            qcolumns.push(new HBaseTypes.TColumnValue(ele));
        });
        query.columnValues = qcolumns;
    }

//    console.log(query,'--------');

    var tPut = new HBaseTypes.TPut(query);

    this.client.put(table, tPut, function (err) {

        if (err) {
            callback(err);
        } else {
            callback(null);
        }
    });
};

Client.prototype.putRow = function (table, row, columns, value, timestamp, callback) {
    var args = arguments;
    var query = {};

    if (args.length <= 0) {
        console.log('arguments arg short of 5');
        return;
    }
    var callback = args[args.length - 1];
    if (callback && typeof callback != 'function') {
        console.log('callback is not a function');
        return;
    }
    if (args.length < 5) {
        callback(new Error('arguments arg short of 5'));
        return;
    }

    if (args.length >= 5) {
        if (args[2].indexOf(':') == -1) {
            callback(new Error('family and qualifier must have it,example ["info:name"]'));
            return;
        }
    }


    query.row = row;
    var qcolumns = [];
    if (columns) {
        var cols = [], temp = {};
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

//    console.log(query);


    var tPut = new HBaseTypes.TPut(query);

    this.client.put(table, tPut, function (err) {

        if (err) {
            callback(err);
        } else {
            callback(null);
        }
    });
};

Client.prototype.del = function (table, param, callback) {
    var tDelete = new HBaseTypes.TDelete(param);
    this.client.deleteSingle(table, tDelete, function (err) {

        if (err) {
            callback(err);
        } else {
            callback(null);
        }
    });
};

Client.prototype.delRow = function (table, row, columns, timestamp, callback) {
    var args = arguments;
    var query = {};

    if (args.length <= 0) {
        console.log('arguments arg short of 3');
        return;
    }
    var callback = args[args.length - 1];
    if (callback && typeof callback != 'function') {
        console.log('callback is not a function');
        return;
    }
    if (args.length < 3) {
        callback(new Error('arguments arg short of 3'));
        return;
    }

    if (args.length === 5) {
        if (args[2].indexOf(':') == -1) {
            callback(new Error('family and qualifier must have it,example ["info:name"]'));
            return;
        }
    }


    query.row = row;
    var qcolumns = [];
    if (args.length >= 4 && columns) {
        var cols = [], temp = {};
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

//    console.log(query);


    var tDelete = new HBaseTypes.TDelete(query);

    this.client.deleteSingle(table, tDelete, function (err, data) {
        if (err) {
            callback(err);
        } else {
            callback(null, data);
        }
    });
};

Client.prototype.inc = function (table, param, callback) {
    var row = param.row;
    if (!row) {
        callback(new Error('rowKey is null'));
    }
    var query = {};

    query.row = row;
    var qcolumns = [];
    if (param.columns && param.columns.length > 0) {
        _.each(param.columns, function (ele, idx) {
            qcolumns.push(new HBaseTypes.TColumnIncrement(ele));
        });
        query.columns = qcolumns;
    }


    var tIncrement = new HBaseTypes.TIncrement(query);

    this.client.increment(table, tIncrement, function (err, data) {

        if (err) {
            callback(err);
        } else {
            callback(null, data);
        }
    });

};

Client.prototype.incRow = function (table, row, columns, callback) {
    var args = arguments;
    var query = {};

    if (args.length <= 0) {
        console.log('arguments arg short of 3');
        return;
    }
    var callback = args[args.length - 1];
    if (callback && typeof callback != 'function') {
        console.log('callback is not a function');
        return;
    }
    if (args.length < 3) {
        callback(new Error('arguments arg short of 3'));
        return;
    }

    if (args.length >= 3) {
        if (args[2].indexOf(':') == -1) {
            callback(new Error('family and qualifier must have it,example ["info:counter"]'));
            return;
        }
    }


    query.row = row;
    var qcolumns = [];
    if (columns) {
        var cols = [], temp = {};
        cols = columns.split(':');
        temp = {
            family: cols[0],
            qualifier: cols[1]
        };
        qcolumns.push(new HBaseTypes.TColumn(temp));
        query.columns = qcolumns;
    }

//    console.log(query);

    var tIncrement = new HBaseTypes.TIncrement(query);

    this.client.increment(table, tIncrement, function (err, data) {
        if (err) {
            callback(err);
        } else {
            callback(null, data);
        }
    });


};

module.exports = ClientPool;