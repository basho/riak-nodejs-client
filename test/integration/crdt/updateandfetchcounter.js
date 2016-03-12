'use strict';

var assert = require('assert');

var Test = require('../testparams');
var UpdateCounter = require('../../../lib/commands/crdt/updatecounter');
var FetchCounter = require('../../../lib/commands/crdt/fetchcounter');

describe('Update and Fetch Counter - Integration', function() {
    var cluster;
    before(function(done) {
        cluster = Test.buildCluster(function (err, rslt) {
            assert(!err, err);
            var callback = function(err, resp) {
                assert(!err, err);
                done();
            };
            var update = new UpdateCounter.Builder()
                    .withBucketType(Test.counterBucketType)
                    .withBucket(Test.bucketName)
                    .withKey('counter_1')
                    .withIncrement(10)
                    .withCallback(callback)
                    .build();
            cluster.execute(update);
        });
    });
    
    after(function(done) {
        Test.cleanBucket(cluster, Test.counterBucketType, Test.bucketName, function() {
            cluster.stop(function (err, rslt) {
                assert(!err, err);
                done();
            });
        });
    });
    
    it('Should fetch a counter', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.counterValue, 10);
            done();
        };
        var fetch = new FetchCounter.Builder()
            .withKey('counter_1')
            .withBucketType(Test.counterBucketType)
            .withBucket(Test.bucketName)
            .withCallback(callback)
            .build();
        cluster.execute(fetch);
    });
    
    it('Should report isNotFound if a counter does not exist', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp.notFound);
            assert(resp.isNotFound);
            done();
        };
        var fetch = new FetchCounter.Builder()
            .withKey('counter_notFound')
            .withBucketType(Test.counterBucketType)
            .withBucket(Test.bucketName)
            .withCallback(callback)
            .build();
        cluster.execute(fetch);
    });
    
    it('Should update a counter', function(done) {
       
       var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.counterValue, 20);
            done();
        };
        
        var update = new UpdateCounter.Builder()
                .withKey('counter_1')
                .withBucketType(Test.counterBucketType)
                .withBucket(Test.bucketName)
                .withIncrement(10)
                .withCallback(callback)
                .build();
        
        cluster.execute(update);
        
    });
    
    it('Should generate a key if one isn\'t supplied', function(done) {
       
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.counterValue, 10);
            assert(resp.generatedKey);
            done();
        };
        
        var update = new UpdateCounter.Builder()
                .withBucketType(Test.counterBucketType)
                .withBucket(Test.bucketName)
                .withIncrement(10)
                .withCallback(callback)
                .build();
        
        cluster.execute(update);
        
    });
    
});
