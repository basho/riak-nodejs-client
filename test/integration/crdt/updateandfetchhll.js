'use strict';

var assert = require('assert');

var Test = require('../testparams');
var UpdateHll = require('../../../lib/commands/crdt/updatehll');
var FetchHll = require('../../../lib/commands/crdt/fetchhll');
var RiakCluster = require('../../../lib/core/riakcluster');

describe('Update and Fetch Hyperloglog - Integration', function() {
    var context;
    var cluster;
    before(function(done) {
        cluster = Test.buildCluster(function (err, rslt) {
            assert(!err, err);
            var callback = function(err, resp) {
                assert(!err, err);
                assert.equal(resp.cardinality, 4);

                done();
            };
            var update = new UpdateHll.Builder()
                    .withBucketType(Test.hllBucketType)
                    .withBucket(Test.bucketName)
                    .withKey('hll_1')
                    .withAdditions(['Jokes', 'are', 'better', 'explained', 'Jokes'])
                    .withCallback(callback)
                    .build();
            cluster.execute(update);
        });
    });

    after(function(done) {
        Test.cleanBucket(cluster, Test.hllBucketType, Test.bucketName, function() {
            cluster.stop(function (err, state) {
                done();
            });
        });
    });

    it('Should fetch a hyperloglog', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert(!resp.notFound);
            assert(!resp.isNotFound);
            assert.equal(resp.cardinality, 4);
            done();
        };
        var fetch = new FetchHll.Builder()
                .withBucketType(Test.hllBucketType)
                .withBucket(Test.bucketName)
                .withKey('hll_1')
                .withCallback(callback)
                .build();
        cluster.execute(fetch);
    });

    it('Should report isNotFound if a hyperloglog does not exist', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp.isNotFound);
            assert.equal(resp.cardinality, null);
            done();
        };
        var fetch = new FetchHll.Builder()
                .withBucketType(Test.hllBucketType)
                .withBucket(Test.bucketName)
                .withKey('empty_hll')
                .withCallback(callback)
                .build();
        cluster.execute(fetch);
    });

});
