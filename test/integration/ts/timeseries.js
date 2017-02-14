/**
 *
 * Copyright 2014-present Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

var Test = require('../testparams');
var TS = require('../../../lib/commands/ts');
var utils = require('../../../lib/utils');

var assert = require('assert');
var logger = require('winston');
var crypto = require('crypto');
var rs = require('randomstring');

var tableName = 'GeoCheckin';
var blobTableName = tableName + rs.generate(8);

// NB: ends with 987ms to test ms resolution
var now = 1443796900987;
var fiveMinsInMsec = 5 * 60 * 1000;
var fiveMinsAgo = now - fiveMinsInMsec;
var tenMinsAgo = fiveMinsAgo - fiveMinsInMsec;
var fifteenMinsAgo = tenMinsAgo - fiveMinsInMsec;
var twentyMinsAgo = fifteenMinsAgo - fiveMinsInMsec;

var columns = [
    { name: 'geohash',     type: TS.ColumnType.Varchar },
    { name: 'user',        type: TS.ColumnType.Varchar },
    { name: 'time',        type: TS.ColumnType.Timestamp },
    { name: 'weather',     type: TS.ColumnType.Varchar },
    { name: 'temperature', type: TS.ColumnType.Double }
];

var blobColumns = columns.concat({ name: 'sensor_data', type: TS.ColumnType.Blob });

var rows = [
    [ 'hash1', 'user2', twentyMinsAgo, 'hurricane', 82.3 ],
    [ 'hash1', 'user2', fifteenMinsAgo, 'rain', 79.0 ],
    [ 'hash1', 'user2', fiveMinsAgo, 'wind', null ],
    [ 'hash1', 'user2', now, 'snow', 20.1 ]
];

var blobRows = [
    rows[0].concat([null]),
    rows[1].concat([null]),
    rows[2].concat([crypto.randomBytes(16)]),
    rows[3].concat([crypto.randomBytes(16)])
];

var cluster;
var ts_supported = true;

function validateResponseRow(got, want) {
    assert.equal(got.length, want.length);
    assert.strictEqual(got[0].toString('utf8'), 'hash1');
    assert.strictEqual(got[1].toString('utf8'), 'user2');
    assert(got[2].equals(fiveMinsAgo));
    assert.strictEqual(got[3].toString('utf8'), 'wind');
    assert.strictEqual(got[4], null);
}

function storeTestData(tableName, columns, rows) {
    return function(done) {
        var suite = this;
        cluster = Test.buildCluster(function (err, rslt) {
            assert(!err, err);
            var callback = function(err, resp) {
                if (err) {
                    ts_supported = false;
                    suite.skip();
                } else {
                    assert(resp);
                }
                done();
            };
            var store = new TS.Store.Builder()
                .withTable(tableName)
                .withColumns(columns)
                .withRows(rows)
                .withCallback(callback)
                .build();
            cluster.execute(store);
        });
    };
}

describe('timeseries-integration', function () {
    before(storeTestData(tableName, columns, rows));

    describe('query', function () {
        before(function(done) {
            if (!ts_supported) {
                this.skip();
            }
            done();
        });
        it('describes-table', function(done) {
            var queryText = 'DESCRIBE GeoCheckin';
            var callback = function(err, resp) {
                assert(!err, err);
                assert(resp.columns.length >= 5);
                assert.strictEqual(resp.rows.length, 5);
                done();
            };
            var q = new TS.Query.Builder()
                .withQuery(queryText)
                .withCallback(callback)
                .build();
            cluster.execute(q);
        });
        it('creates-table', function(done) {
            var queryText = 'CREATE TABLE ' + blobTableName +
                '(geohash varchar not null, user varchar not null, time timestamp not null, weather varchar not null, temperature double, sensor_data BLOB, ' +
                'PRIMARY KEY((geohash, user, quantum(time, 15, m)), geohash, user, time))';
            var callback = function(err, resp) {
                assert(!err, err);
                assert.strictEqual(resp.columns.length, 0);
                assert.strictEqual(resp.rows.length, 0);
                done();
            };
            var q = new TS.Query.Builder()
                .withQuery(queryText)
                .withCallback(callback)
                .build();
            cluster.execute(q);
        });
        it('no-matches-returns-no-data', function(done) {
            var queryText = "select * from GeoCheckin where time > 0 and time < 10 and geohash = 'hash1' and user = 'user1'";
            var callback = function(err, resp) {
                assert(!err, err);
                assert.strictEqual(resp.columns.length, 0);
                assert.strictEqual(resp.rows.length, 0);
                done();
            };
            var q = new TS.Query.Builder()
                .withQuery(queryText)
                .withCallback(callback)
                .build();
            cluster.execute(q);
        });
        it('some-matches-returns-data', function(done) {
            var queryText = "select * from GeoCheckin where time > " + tenMinsAgo +
                            " and time < " + now +
                            " and geohash = 'hash1' and user = 'user2'";
            var callback = function(err, resp) {
                assert(!err, err);
                assert.strictEqual(resp.columns.length, columns.length);
                var got = resp.rows[0];
                var want = rows[2];
                validateResponseRow(got, want);
                done();
            };
            var q = new TS.Query.Builder()
                .withQuery(queryText)
                .withCallback(callback)
                .build();
            cluster.execute(q);
        });
    });

    describe('list-keys', function () {
        before(function(done) {
            if (!ts_supported) {
                this.skip();
            }
            done();
        });
        it('returns-all-keys-no-streaming', function(done) {
            var callback = function(err, resp) {
                assert(!err, err);
                assert(resp.keys.length >= rows.length);
                var keysFound = 0;
                resp.keys.forEach(function (key) {
                    assert.strictEqual(key.length, 3);
                    if (key[0] == 'hash1') {
                        assert.equal(key[0], 'hash1');
                        assert.equal(key[1], 'user2');
                        assert(utils.isInteger(key[2]));
                        keysFound++;
                    }
                });
                assert(keysFound >= rows.length);
                done();
            };
            var cmd = new TS.ListKeys.Builder()
                .withAllowListing()
                .withTable(tableName)
                .withStreaming(false)
                .withCallback(callback)
                .build();
            cluster.execute(cmd);
        });
        it('returns-all-keys-streaming', function(done) {
            var allKeys = [];
            var callback = function(err, resp) {
                assert(!err, err);
                Array.prototype.push.apply(allKeys, resp.keys);
                if (resp.done) {
                    assert(allKeys.length >= rows.length);
                    done();
                }
            };
            var cmd = new TS.ListKeys.Builder()
                .withAllowListing()
                .withTable(tableName)
                .withStreaming(true)
                .withCallback(callback)
                .build();
            cluster.execute(cmd);
        });
    });

    describe('describe', function () {
        before(function(done) {
            if (!ts_supported) {
                this.skip();
            }
            done();
        });
        it('returns-table-description', function(done) {
            var callback = function(err, resp) {
                assert(!err, err);
                assert(resp.columns.length >= 5);
                assert.strictEqual(resp.rows.length, 5);
                done();
            };
            var cmd = new TS.Describe.Builder()
                .withTable(tableName)
                .withCallback(callback)
                .build();
            cluster.execute(cmd);
        });
    });

    describe('Get', function () {
        before(function(done) {
            if (!ts_supported) {
                this.skip();
            }
            done();
        });
        it('returns one row of TS data', function(done) {
            var callback = function(err, resp) {
                assert(!err, err);
                assert.equal(resp.columns.length, columns.length);
                var got = resp.rows[0];
                var want = rows[2];
                validateResponseRow(got, want);
                done();
            };
            var key = [ 'hash1', 'user2', fiveMinsAgo ];
            var cmd = new TS.Get.Builder()
                .withTable(tableName)
                .withKey(key)
                .withCallback(callback)
                .build();
            cluster.execute(cmd);
        });
        it('returns-empty-result-when-not-found', function(done) {
            var callback = function(err, resp) {
                assert(!err, err);
                assert.strictEqual(resp.columns.length, 0);
                assert.strictEqual(resp.rows.length, 0);
                done();
            };
            var key = [ 'hash-foo', 'user-bar', fiveMinsAgo ];
            var cmd = new TS.Get.Builder()
                .withTable(tableName)
                .withKey(key)
                .withCallback(callback)
                .build();
            cluster.execute(cmd);
        });
        it('returns error for incorrect cell count in key', function(done) {
            var callback = function(err, resp, errdata) {
                assert(err);
                assert(!resp);
                assert(errdata);
                assert(errdata.msg, 'expected an error message');
                assert.strictEqual(errdata.code, 1011);
                done();
            };
            var key = [ 'hash1', 'user2' ];
            var cmd = new TS.Get.Builder()
                .withTable(tableName)
                .withKey(key)
                .withCallback(callback)
                .build();
            cluster.execute(cmd);
        });
    });

    describe('Delete', function () {
        before(function(done) {
            if (!ts_supported) {
                this.skip();
            }
            done();
        });
        it('deletes-one-row-of-ts-data', function(done) {
            var key = [ 'hash1', 'user2', twentyMinsAgo ];
            var cb2 = function(err, resp, errdata) {
                assert(!err, err);
                assert.strictEqual(resp.columns.length, 0);
                assert.strictEqual(resp.rows.length, 0);
                done();
            };
            var cb1 = function(err, resp) {
                assert(!err, err);
                assert(resp);
                var cmd = new TS.Get.Builder()
                    .withTable(tableName)
                    .withKey(key)
                    .withCallback(cb2)
                    .build();
                cluster.execute(cmd);
            };
            var cmd = new TS.Delete.Builder()
                .withTable(tableName)
                .withKey(key)
                .withCallback(cb1)
                .build();
            cluster.execute(cmd);
        });
    });

    describe('BlobType', function () {
        before(storeTestData(blobTableName, blobColumns, blobRows));
        it('can-decode-blob-cells', function(done) {
            var queryText = "select * from GeoCheckin where time > " + tenMinsAgo +
                            " and time < " + now +
                            " and geohash = 'hash1' and user = 'user2'";
            var callback = function(err, resp) {
                assert(!err, err);
                assert.strictEqual(resp.columns.length, columns.length);
                var got = resp.rows[0];
                var want = rows[3];
                validateResponseRow(got, want);
                assert.strictEqual(got[5], want[5]);
                done();
            };
            var q = new TS.Query.Builder()
                .withQuery(queryText)
                .withCallback(callback)
                .build();
            cluster.execute(q);
        });
    });

    after(function (done) {
        cluster.stop(function (err, rslt) {
            assert(!err, err);
            done();
        });
    });
});
