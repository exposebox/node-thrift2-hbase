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
        if (this._scannerClosed) return;

        return new Promise((resolve, reject) => {
            this.hbaseClientPool.acquire((aquireError, hbaseClient) => {
                if (aquireError) {
                    return this.closeScanner(aquireError, resolve);
                }

                const hbaseThriftClient = hbaseClient.thriftClient;

                this.hbaseClient = hbaseClient;
                this.hbaseThriftClient = hbaseThriftClient;

                if (this._scannerClosed) {
                    return this.closeScanner(undefined, resolve);
                }

                hbaseThriftClient.openScanner(this.table, this.tScan, (openScannerError, scannerId) => {
                    if (openScannerError) {
                        return this.closeScanner(openScannerError, resolve);
                    }

                    this.scannerId = scannerId;

                    if (this._scannerClosed) {
                        return this.closeScanner(undefined, resolve);
                    }

                    resolve();
                });
            });
        });
    }

    async _read() {
        if (this._scannerClosed) return;

        if (!this._readStarted) {
            await this.startScan();

            if (this._scannerClosed) {
                return this.closeScanner(undefined);
            }

            this._readStarted = true;
        }

        this.hbaseThriftClient
            .getScannerRows(this.scannerId, this.scan.chunkSize, (scanError, data) => {
                //  error
                if (scanError) {
                    return this.closeScanner(scanError);
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

    async closeScanner(closeByError, scannerClosedCallback) {
        this._scannerClosed = true;

        if (this.scannerId !== undefined) {
            try {
                await new Promise((resolve, reject) =>
                    this.hbaseThriftClient.closeScanner(this.scannerId, err => !err ? resolve() : reject(err)));

                this.scannerId = undefined;
            } catch (err) {
                this.emit('error', err);
            }
        }

        if (this.hbaseClient !== undefined) {
            try {
                this.hbaseClientPool.release(this.hbaseClient);

                this.hbaseClient = undefined;
            } catch (err) {
                this.emit('error', err);
            }
        }

        if (closeByError !== undefined) {
            this.emit('error', closeByError);
        }

        try {
            scannerClosedCallback && scannerClosedCallback();
        } catch (err) {
            this.emit('error', err);
        }
    }
}

module.exports = ScanStream;