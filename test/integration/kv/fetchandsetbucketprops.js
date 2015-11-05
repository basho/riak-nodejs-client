'use strict';

var Test = require('../testparams');
var StoreProps = require('../../../lib/commands/kv/storebucketprops');
var FetchProps = require('../../../lib/commands/kv/fetchbucketprops');
var RiakNode = require('../../../lib/core/riaknode');
var RiakCluster = require('../../../lib/core/riakcluster');
var assert = require('assert');

describe('Store and Fetch Bucket props - Integration', function() {
   
   this.timeout(10000);
   
    var cluster;
    
    before(function(done) {
        var nodes = RiakNode.buildNodes(Test.nodeAddresses);
        cluster = new RiakCluster({ nodes: nodes});
        cluster.start();
        done();
    });
    
    after(function(done) {
        
        cluster.on('stateChange', function(state) { if (state === RiakCluster.State.SHUTDOWN) { done();} });
        cluster.stop();
            
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

