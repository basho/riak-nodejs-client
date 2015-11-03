'use strict';

var Test = require('../testparams');

var TS = require('../../../lib/commands/ts');
var RiakNode = require('../../../lib/core/riaknode');
var RiakCluster = require('../../../lib/core/riakcluster');

var assert = require('assert');
var logger = require('winston');

var tableName = 'GeoCheckin';
var now = 1443796900000; // Let's just pretend this is the value of Date.now();
var fiveMinsInMsec = 5 * 60 * 1000;
var fiveMinsAgo = now - fiveMinsInMsec;
var tenMinsAgo = fiveMinsAgo - fiveMinsInMsec;
var fifteenMinsAgo = tenMinsAgo - fiveMinsInMsec;
var twentyMinsAgo = fifteenMinsAgo - fiveMinsInMsec;

/*
CREATE TABLE GeoCheckin (
    geohash varchar not null,
    user varchar not null,
    time timestamp not null,
    weather varchar not null,
    temperature float,
    PRIMARY KEY((geohash, user, quantum(time, 15, m)), geohash, user, time)
)
*/
var columns = [
    { name: 'geohash',     type: TS.ColumnType.Binary },
    { name: 'user',        type: TS.ColumnType.Binary },
    { name: 'time',        type: TS.ColumnType.Timestamp },
    { name: 'weather',     type: TS.ColumnType.Binary },
    { name: 'temperature', type: TS.ColumnType.Float }
];

// TODO FUTURE - when this PR is accepted, it will be OK to have
// floats that end in .0
// https://github.com/basho/riak_pb/pull/135
var rows = [
    [ 'hash1', 'user2', twentyMinsAgo, 'hurricane', 82.3 ],
    [ 'hash1', 'user2', fifteenMinsAgo, 'rain', 79.0 ],
    [ 'hash1', 'user2', fiveMinsAgo, 'wind', null ],
    [ 'hash1', 'user2', now, 'snow', 20.1 ]
];

var cluster;

describe('Timeseries - Integration', function () {
    this.timeout(10000);

    before(function(done) {
        var nodes = RiakNode.buildNodes(Test.nodeAddresses);
        cluster = new RiakCluster({ nodes: nodes});
        cluster.start();

        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp);
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

    describe('Query', function () {
        it('no matches returns no data', function(done) {
            var queryText = "select * from GeoCheckin where time > 0 and time < 10 and user = 'user1'";
            logger.debug("query 1", queryText);
            var callback = function(err, resp) {
                assert(!err, err);
                assert.equal(resp.columns.length, 0);
                assert.equal(resp.rows.length, 0);
                done();
            };
            var q = new TS.Query.Builder()
                .withQuery(queryText)
                .withCallback(callback)
                .build();
            cluster.execute(q);
        });
        it('some matches returns data', function(done) {
            var queryText = "select * from GeoCheckin where time > " + tenMinsAgo +
                            " and time < " + now +
                            " and user = 'user2'";
            logger.debug("query 2", queryText);
            var callback = function(err, resp) {
                assert(!err, err);
                assert.equal(resp.columns.length, 5);
                assert.equal(resp.rows.length, 1);
                var r0 = resp.rows[0];
                assert.equal(r0[0], 'hash1');
                assert.equal(r0[1], 'user2');
                assert.equal(r0[2], fiveMinsAgo);
                assert.equal(r0[3], 'wind');
                assert(!r0[4], 'expected null value');
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
        cluster.on('stateChange', function (state) {
            if (state === RiakCluster.State.SHUTDOWN) {
                done();
            }
        });
        cluster.stop();
    });
});
