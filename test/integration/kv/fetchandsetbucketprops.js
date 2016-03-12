'use strict';

var assert = require('assert');

var Test = require('../testparams');
var StoreProps = require('../../../lib/commands/kv/storebucketprops');
var FetchProps = require('../../../lib/commands/kv/fetchbucketprops');

describe('Store and Fetch Bucket props - Integration', function() {
    var cluster;
    before(function(done) {
        cluster = Test.buildCluster(function (err, rslt) {
            assert(!err, err);
            done();
        });
    });
    
    after(function(done) {
        cluster.stop(function (err, rslt) {
            assert(!err, err);
            done();
        });
    });
    
    describe('StoreBucketProps', function() {
        it('Should store props for a bucket in the default type', function(done) {
            var callback = function(err, resp) {
                assert(!err, err);
                done();
            };
            var store = new StoreProps.Builder()
                    .withBucket(Test.bucketName + '_sp')
                    .withAllowMult(true)
                    .withCallback(callback)
                    .build();
            cluster.execute(store);
        });
    
        it('Should store bucket props for a bucket in a non-default type', function(done) {
            var callback = function(err, resp) {
                assert(!err, err);
                done();
            };
            var store = new StoreProps.Builder()
                    .withBucketType(Test.bucketType)
                    .withBucket(Test.bucketName + '_sp')
                    .withAllowMult(false)
                    .withCallback(callback)
                    .build();
            cluster.execute(store);

        });
    });
    
    describe('FetchBucketProps', function() {
        it('Should fetch props for a bucket in the default Type', function(done) {
            var callback = function(err, resp) {
                assert(!err, err);
                assert(resp.nVal);
                done();
            };
            var fetch = new FetchProps.Builder()
                .withBucket(Test.bucketName + '_sp')
                .withCallback(callback)
                .build();
            cluster.execute(fetch);
        });
        
        it('Should fetch props for a bucket in a non-default Type', function(done) {
            var callback = function(err, resp) {
                assert(!err, err);
                assert(resp.nVal);
                done();
            };
            var fetch = new FetchProps.Builder()
                .withBucketType(Test.bucketType)
                .withCallback(callback)
                .withBucket(Test.bucketName + '_sp')
                .build();
            cluster.execute(fetch);
        });
    });
});
