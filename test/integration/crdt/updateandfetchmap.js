'use strict';

var assert = require('assert');

var Test = require('../testparams');
var UpdateMap = require('../../../lib/commands/crdt/updatemap');
var FetchMap = require('../../../lib/commands/crdt/fetchmap');

describe('Update and Fetch Map - Integration', function() {
    var context;
    var cluster;
    before(function(done) {
        cluster = Test.buildCluster(function (err, rslt) {
            assert(!err, err);
            var callback = function(err, resp) {
                assert(!err, err);
                assert(resp.context);
                context = resp.context;
                done();
            };

            var mapOp = new UpdateMap.MapOperation();
            mapOp.incrementCounter('counter_1', 50)
                .addToSet('set_1', 'value_1')
                .setRegister('register_1', new Buffer('register_value_1'))
                .setFlag('flag_1', true);

            mapOp.map('map_2').incrementCounter('counter_1', 50)
                .addToSet('set_1', 'value_1')
                .setRegister('register_1', new Buffer('register_value_1'))
                .setFlag('flag_1', true)
                .map('map_3');

            var update = new UpdateMap.Builder()
                .withBucketType(Test.mapBucketType)
                .withBucket(Test.bucketName)
                .withKey('map_1')
                .withMapOperation(mapOp)
                .withCallback(callback)
                .withReturnBody(true)
                .withTimeout(20000)
                .build();
            cluster.execute(update);
        });
    });
    
    after(function(done) {
        Test.cleanBucket(cluster, Test.mapBucketType, Test.bucketName, function() {
            cluster.stop(function (err, rslt) {
                assert(!err, err);
                done();
            });
        });
    });
    
    it('Should fetch a map', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp.context);
            assert(resp.map);
            assert.equal(resp.map.counters.counter_1, 50);
            assert.equal(resp.map.sets.set_1[0], 'value_1');
            assert.equal(resp.map.registers.register_1.toString('utf8'), 'register_value_1');
            assert.equal(resp.map.flags.flag_1, true);
            done();
        };
        var fetch = new FetchMap.Builder()
				.withBucketType(Test.mapBucketType)
                .withBucket(Test.bucketName)
                .withKey('map_1')
				.withCallback(callback)
				.build();
        cluster.execute(fetch);
    });
    
    it('Should report isNotFound if a map does not exist', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp.notFound);
            assert(resp.isNotFound);
            done();
        };
        var fetch = new FetchMap.Builder()
				.withBucketType(Test.mapBucketType)
                .withBucket(Test.bucketName)
                .withKey('map_notFound')
				.withCallback(callback)
				.build();
        cluster.execute(fetch);
    });
    
    it('Should remove stuff from a map', function(done) {
        
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp.context);
            assert(resp.map);
            assert(!resp.map.counters.counter_1);
            assert.equal(resp.map.sets.set_1[0], 'value_1');
            assert.equal(resp.map.registers.register_1.toString('utf8'), 'register_value_1');
            assert.equal(resp.map.flags.flag_1, true);
            done();
        };
        
        var mapOp = new UpdateMap.MapOperation().removeCounter('counter_1');
        
        var update = new UpdateMap.Builder()
            .withBucketType(Test.mapBucketType)
            .withBucket(Test.bucketName)
            .withKey('map_1')
            .withContext(context)
            .withMapOperation(mapOp)
            .withCallback(callback)
            .withTimeout(20000)
            .build();
    
        cluster.execute(update);
        
        
    });
    
});
