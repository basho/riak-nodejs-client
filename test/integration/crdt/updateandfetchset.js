'use strict';

var Test = require('../testparams');
var UpdateSet = require('../../../lib/commands/crdt/updateset');
var FetchSet = require('../../../lib/commands/crdt/fetchset');
var RiakNode = require('../../../lib/core/riaknode');
var RiakCluster = require('../../../lib/core/riakcluster');
var assert = require('assert');

describe('Update and Fetch Set - Integration', function() {
    
    var cluster;
    this.timeout(10000);
    var context;
    
    before(function(done) {
        var nodes = RiakNode.buildNodes(Test.nodeAddresses);
        cluster = new RiakCluster({ nodes: nodes});
        cluster.start(function (err, rslt) {
            assert(!err, err);

            var callback = function(err, resp) {
                assert(!err, err);
                assert.equal(resp.values.length, 2);
                assert(resp.context);
                context = resp.context;
                done();
            };
            
            var update = new UpdateSet.Builder()
                    .withBucketType(Test.setBucketType)
                    .withBucket(Test.bucketName)
                    .withKey('set_1')
                    .withAdditions(['this', 'that'])
                    .withCallback(callback)
                    .build();
            cluster.execute(update);
        });
    });
    
    after(function(done) {
        Test.cleanBucket(cluster, Test.setBucketType, Test.bucketName, function() {
            cluster.on('stateChange', function(state) { if (state === RiakCluster.State.SHUTDOWN) { done();} });
            cluster.stop();
        });
    });
    
    it('Should fetch a set', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.values.length, 2);
            assert(resp.context);
            done();
        };
        var fetch = new FetchSet.Builder()
                .withBucketType(Test.setBucketType)
                .withBucket(Test.bucketName)
                .withKey('set_1')
                .withCallback(callback)
                .build();
        cluster.execute(fetch);
    });
    
    it('Should report isNotFound if a set does not exist', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp.notFound);
            assert(resp.isNotFound);
            done();
        };
        var fetch = new FetchSet.Builder()
                .withBucketType(Test.setBucketType)
                .withBucket(Test.bucketName)
                .withKey('set_notFound')
                .withCallback(callback)
                .build();
        cluster.execute(fetch);
    });
    
    it('Should remove from a set', function(done) {
       
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.values.length, 1);
            done();
        };
        
        // This ... tests updating a new set
        var update = new UpdateSet.Builder()
                .withBucketType(Test.setBucketType)
                .withBucket(Test.bucketName)
                .withKey('set_1')
                .withRemovals(['this'])
                .withContext(context)
                .withCallback(callback)
                .build();
        
        cluster.execute(update);
        
    });

});
