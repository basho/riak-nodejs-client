'use strict';

var rpb = require('../../../lib/protobuf/riakprotobuf');
var ListBuckets = require('../../../lib/commands/kv/listbuckets');
var RpbListBucketsResp = rpb.getProtoFor('RpbListBucketsResp');
var RpbErrorResp = rpb.getProtoFor('RpbErrorResp');

var assert = require('assert');

describe('ListBuckets', function() {
    describe('Build', function() {
        it('should build a RpbListBucketsReq correctly', function(done) {
            
            var listBuckets = new ListBuckets.Builder()
                    .withBucketType('bucket_type')
                    .withCallback(function(){})
                    .withTimeout(30000)
                    .build();
            
            var protobuf = listBuckets.constructPbRequest();
            
            assert.equal(protobuf.getType().toString('utf8'), 'bucket_type');
            // We should always be streaming from Riak
            assert.equal(protobuf.stream, true);
            assert.equal(protobuf.timeout, 30000);
            done();
            
        });
        
        it('should take multiple RpbListBucketsResp and call the users callback with the response', function(done) {
           
            var callback = function(err, resp){
                assert(!err, err);
                assert.equal(resp.buckets.length, 100);
                assert.equal(resp.done, true);
                done();
            };
            
            var listBuckets = new ListBuckets.Builder()
                    .withBucketType('bucket_type')
                    .withStreaming(false)
                    .withCallback(callback)
                    .build();
            
            for (var i = 0; i < 20; i++) {
                var listBucketsResp = new RpbListBucketsResp();    
                for (var j = 0; j < 5; j++) {
                    listBucketsResp.buckets.push(new Buffer('bucket'));
                }
                if (i === 19) {
                    listBucketsResp.done = true;
                }
                listBuckets.onSuccess(listBucketsResp);
            }
            
        });
        
        it('should take multiple RpbListBucketsResp and stream the response', function(done) {
           
            var count = 0;
            var timesCalled = 0;
            var callback = function(err, resp){
                
                timesCalled++;
                count += resp.buckets.length;
                if (resp.done) {
                    assert.equal(timesCalled, 20);
                    assert.equal(count, 100);
                    done();
                }
                
            };
            
            var listBuckets = new ListBuckets.Builder()
                    .withBucketType('bucket_type')
                    .withCallback(callback)
                    .build();
            
            for (var i = 0; i < 20; i++) {
                var listBucketsResp = new RpbListBucketsResp();    
                for (var j = 0; j < 5; j++) {
                    listBucketsResp.buckets.push(new Buffer('bucket'));
                }
                if (i === 19) {
                    listBucketsResp.done = true;
                }
                listBuckets.onSuccess(listBucketsResp);
            }
            
        });
        
        it ('should take a RpbErrorResp and call the users callback with the error message', function(done) {
           var rpbErrorResp = new RpbErrorResp();
           rpbErrorResp.setErrmsg(new Buffer('this is an error'));
           
           var callback = function(err, response) {
               if (err) {
                   assert.equal(err,'this is an error');
                   done();
               }
           };
           
           var listBuckets = new ListBuckets.Builder()
                    .withBucketType('bucket_type')
                    .withStreaming(false)
                    .withCallback(callback)
                    .build();
       
            listBuckets.onRiakError(rpbErrorResp);
           
           
       });
        
    });
});
