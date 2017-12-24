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

                const hbaseThriftClient = hbaseClient.client;

                this.hbaseClient = hbaseClient;
                this.hbaseThriftClient = hbaseThriftClient;

                hbaseThriftClient.openScanner(this.table, this.tScan, (openScannerError, scannerId) => {
                    this.scannerId = scannerId;

                    if (openScannerError) {
                        this.closeScanner(openScannerError, resolve);

                        return;
                    }

                    return resolve();
                });
            });
        });
    }

    async _read() {
        if (!this._readStarted) {
            await this.startScan();

            this._readStarted = true;
        }

        this.hbaseThriftClient
            .getScannerRows(this.scannerId, this.scan.chunkSize, (scanError, data) => {
                //  error
                if (scanError) {
                    this.closeScanner(scanError);

                    return;
                }

                //  end of data
                if (data.length === 0) {
                    this.closeScanner();

                    this.push(null);

                    return;
                }

                //  incoming data
                this.push(this.scan.objectsFromData(data));
            });
    }

    closeScanner(closeByError, scannerClosedCallback) {
        this.hbaseThriftClient
            .closeScanner(this.scannerId, err => {
                if (err) {
                    console.error(err);
                }

                this.hbaseClientPool.release(this.hbaseClient);

                if (scannerClosedCallback !== undefined) {
                    scannerClosedCallback();
                }
            });

        if (closeByError !== undefined) {
            this.emit('error', closeByError.message.slice(0, 120));
        }
    }
}

module.exports = ScanStream;