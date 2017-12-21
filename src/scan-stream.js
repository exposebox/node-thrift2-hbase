'use strict';

const Readable = require('stream').Readable;
const HBaseTypes = require('../gen-nodejs/hbase_types');

class ScanStream extends Readable {
    constructor(hbaseClientPool, table, scan) {
        super({objectMode: true});

        const tScan = new HBaseTypes.TScan(scan);

        Object.assign(this, {hbaseClientPool, table, scan, tScan});
    }

    async startScan() {
        return new Promise((resolve, reject) => {
            this.hbaseClientPool.acquire((err, hbaseClient) => {
                if (err) {
                    this.emit('error', err);

                    return resolve();
                }

                hbaseClient = hbaseClient.client;

                hbaseClient.openScanner(this.table, this.tScan, (openScannerError, scannerId) => {
                    if (openScannerError) {
                        hbaseClient.closeScanner(scannerId, err => {
                            if (err) {
                                console.error(err);
                            }

                            this.emit('error', openScannerError.message.slice(0, 120));

                            return resolve();
                        });
                    } else {
                        this.hbaseClient = hbaseClient;
                        this.scannerId = scannerId;

                        return resolve();
                    }
                });
            });
        });
    }

    async _read() {
        if (!this._readStarted) {
            await this.startScan();

            this._readStarted = true;
        }

        const hbaseClient = this.hbaseClient;
        const scannerId = this.scannerId;

        hbaseClient.getScannerRows(scannerId, this.scan.chunkSize, (serr, data) => {
            if (serr) {
                //  error
                hbaseClient.closeScanner(scannerId, err => {
                    if (err) {
                        console.error(err);
                    }
                });

                this.emit('error', serr.message.slice(0, 120));
            } else {
                if (data.length > 0) {
                    //  incoming data
                    this.push(this.scan.objectsFromData(data));
                } else {
                    //  end of data
                    hbaseClient.closeScanner(scannerId, err => {
                        if (err) {
                            console.error(err);
                        }
                    });

                    this.hbaseClientPool.release(hbaseClient);

                    this.push(null);
                }
            }
        });
    }
}

module.exports = ScanStream;