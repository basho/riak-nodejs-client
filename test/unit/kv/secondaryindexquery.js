'use strict';

var rpb = require('../../../lib/protobuf/riakprotobuf');
var SecondaryIndexQuery = require('../../../lib/commands/kv/secondaryindexquery');
var RpbIndexResp = rpb.getProtoFor('RpbIndexResp');
var RpbIndexReq = rpb.getProtoFor('RpbIndexReq');
var RpbErrorResp = rpb.getProtoFor('RpbErrorResp');
var RpbPair = rpb.getProtoFor('RpbPair');
var assert = require('assert');

describe('SecondaryIndexQuery', function() {
    describe('Build', function() {
        it('should build a RpbIndexReq correctly for a single key query', function(done) {
            
            var siq = new SecondaryIndexQuery.Builder()
                    .withBucket('MyBucket')
                    .withIndexName('email_bin')
                    .withIndexKey('roach@basho.com')
                    .withCallback(function(){})
                    .withTimeout(20000)
                    .withReturnKeyAndIndex(true)
                    .withMaxResults(20)
                    .withContinuation(new Buffer('1234'))
                    .build();
            
            var protobuf = siq.constructPbRequest();
            
            assert.equal(protobuf.getType().toString('utf8'), 'default');
            assert.equal(protobuf.getBucket().toString('utf8'), 'MyBucket');
            // We should always be streaming from Riak
            assert.equal(protobuf.stream, true);
            assert.equal(protobuf.timeout, 20000);
            assert.equal(protobuf.getKey().toString('utf8'), 'roach@basho.com');
            assert.equal(protobuf.getRangeMin(), null);
            assert.equal(protobuf.getRangeMax(), null);
            assert.equal(protobuf.getIndex().toString('utf8'), 'email_bin');
            assert.equal(protobuf.getReturnTerms(), true);
            assert.equal(protobuf.getContinuation().toString('utf8'), '1234');
            assert.equal(protobuf.getQtype(), RpbIndexReq.IndexQueryType.eq);
            assert.equal(protobuf.getMaxResults(), 20);
            done();
            
            
        });

        it('should build a RpbIndexReq correctly for a single _bin key query without string2int conversion', function(done) {
            var siq = new SecondaryIndexQuery.Builder()
                .withBucket('MyBucket')
                .withIndexName('test_bin')
                .withIndexKey('9999999999999999')
                .withCallback(function(){})
                .build();
            var protobuf = siq.constructPbRequest();
            assert.equal(protobuf.getKey().toString('utf8'), '9999999999999999');
            done();
        });

        it('should build a RpbIndexReq correctly for a range query without string2int conversion', function(done) {
            var siq = new SecondaryIndexQuery.Builder()
                .withBucket('MyBucket')
                .withIndexName('test_bin')
                .withRange('9999999999999999', '9999999999999999')
                .withCallback(function(){})
                .build();
            var protobuf = siq.constructPbRequest();
            assert.equal(protobuf.getRangeMin().toString('utf8'), '9999999999999999');
            assert.equal(protobuf.getRangeMax().toString('utf8'), '9999999999999999');
            done();
        });

        it('should build a RpbIndexReq correctly for a range query', function(done) {
            
            var siq = new SecondaryIndexQuery.Builder()
                    .withBucket('MyBucket')
                    .withIndexName('id_int')
                    .withRange(0,50)
                    .withCallback(function(){})
                    .withTimeout(20000)
                    .withReturnKeyAndIndex(true)
                    .withMaxResults(20)
                    .withContinuation(new Buffer('1234'))
                    .build();
            
            var protobuf = siq.constructPbRequest();
            
            assert.equal(protobuf.getType().toString('utf8'), 'default');
            assert.equal(protobuf.getBucket().toString('utf8'), 'MyBucket');
            // We should always be streaming from Riak
            assert.equal(protobuf.stream, true);
            assert.equal(protobuf.timeout, 20000);
            assert.equal(protobuf.getIndex().toString('utf8'), 'id_int');
            assert.equal(protobuf.getReturnTerms(), true);
            assert.equal(protobuf.getContinuation().toString('utf8'), '1234');
            assert.equal(protobuf.getQtype(), RpbIndexReq.IndexQueryType.range);
            assert.equal(protobuf.getKey(), null);
            // Riak's API expects strings, not numbers, for _int index queries
            // The command converts them
            assert.equal(protobuf.getRangeMin().toString('utf8'), '0');
            assert.equal(protobuf.getRangeMax().toString('utf8'), '50');
            done();
        });
        
        it('maxResults should override paginationSort', function(done) {
            
            var siq = new SecondaryIndexQuery.Builder()
                    .withBucket('MyBucket')
                    .withIndexName('id_int')
                    .withRange(0,50)
                    .withCallback(function(){})
                    .withMaxResults(20)
                    .withPaginationSort(true)
                    .build();
            
            var protobuf = siq.constructPbRequest();
            
            assert.equal(protobuf.getPaginationSort(), null);
            
            siq = new SecondaryIndexQuery.Builder()
                    .withBucket('MyBucket')
                    .withIndexName('id_int')
                    .withRange(0,50)
                    .withCallback(function(){})
                    .withPaginationSort(true)
                    .build();
            
            protobuf = siq.constructPbRequest();
            
            assert.equal(protobuf.getPaginationSort(), true);
            done();
            
        });
        
        it('requires either an indexKey or rangeStart + rangeEnd', function(done) {
           
           assert.throws(
                function() { var siq = new SecondaryIndexQuery.Builder()
                            .withBucket('MyBucket')
                            .withIndexName('id_int')
                            .withCallback(function(){})
                            .build();
                },
                'either \'indxKey\' or \'rangeStart\' + \'rangeEnd\' are required'
            );
            done();
            
        });
        
        it('indexKey overrides range if both supplied', function(done) {
           
            var siq = new SecondaryIndexQuery.Builder()
                    .withBucket('MyBucket')
                    .withIndexName('id_int')
                    .withIndexKey(5)
                    .withRange(0,50)
                    .withCallback(function(){})
                    .build();
            
            var protobuf = siq.constructPbRequest();
            
            assert.equal(protobuf.getKey().toString('utf8'), '5');
            assert.equal(protobuf.getRangeMin(), null);
            assert.equal(protobuf.getRangeMax(), null);
            assert.equal(protobuf.getQtype(), RpbIndexReq.IndexQueryType.eq);
            done();
            
        });
        
        it('should take multiple RpbIndexResp with just object keys and call the users callback with the response', function(done) {
            
            var callback = function(err, response) {
                
                assert.equal(response.values.length, 100);
                assert.equal(response.values[0].indexKey, null);
                assert.equal(response.values[0].objectKey, 'object_key');
                assert.equal(response.done, true);
                assert.equal(response.continuation.toString('utf8'), '1234');
                done();
            };
            
            var siq = new SecondaryIndexQuery.Builder()
                    .withBucket('MyBucket')
                    .withIndexName('id_bin')
                    .withCallback(callback)
                    .withStreaming(false)
                    .withRange(0,50)
                    .build();
            
            for (var i = 0; i < 20; i++) {
                var indexResp = new RpbIndexResp();    
                for (var j = 0; j < 5; j++) {
                    indexResp.keys.push(new Buffer('object_key'));
                }
                if (i === 19) {
                    indexResp.done = true;
                    indexResp.continuation = new Buffer('1234');
                }
                siq.onSuccess(indexResp);
            }
        });
        
        it('should take multiple RpbIndexResp with term/key pairs and call the users callback with the response', function(done) {
            
            var callback = function(err, response) {
                
                assert.equal(response.values.length, 100);
                assert.equal(response.values[0].indexKey, 'index_key');
                assert.equal(response.values[0].objectKey, 'object_key');
                assert.equal(response.done, true);
                assert.equal(response.continuation.toString('utf8'), '1234');
                done();
            };
            
            var siq = new SecondaryIndexQuery.Builder()
                    .withBucket('MyBucket')
                    .withIndexName('id_bin')
                    .withCallback(callback)
                    .withStreaming(false)
                    .withReturnKeyAndIndex(true)
                    .withRange(0, 50)
                    .build();
            
            for (var i = 0; i < 20; i++) {
                var indexResp = new RpbIndexResp();    
                for (var j = 0; j < 5; j++) {
                    var pair = new RpbPair();
                    pair.key = new Buffer('index_key');
                    pair.value = new Buffer('object_key');
                    indexResp.results.push(pair);
                }
                if (i === 19) {
                    indexResp.done = true;
                    indexResp.continuation = new Buffer('1234');
                }
                siq.onSuccess(indexResp);
            }
        });
        
        it('should take multiple RpbIndexResp with just object keys and stream the response', function(done) {
            
            var count = 0;
            var timesCalled = 0;
            var callback = function(err, response) {
                
                timesCalled++;
                count += response.values.length;
                
                assert.equal(response.values[0].indexKey, null);
                assert.equal(response.values[0].objectKey, 'object_key');
                
                
                if (response.done) {
                    assert.equal(timesCalled, 20);
                    assert.equal(count, 100);
                    assert.equal(response.continuation.toString('utf8'), '1234');
                    done();
                }
            };
            
            var siq = new SecondaryIndexQuery.Builder()
                    .withBucket('MyBucket')
                    .withIndexName('id_int')
                    .withCallback(callback)
                    .withStreaming(true)
                    .withRange(0, 50)
                    .build();
            
            for (var i = 0; i < 20; i++) {
                var indexResp = new RpbIndexResp();    
                for (var j = 0; j < 5; j++) {
                    indexResp.keys.push(new Buffer('object_key'));
                }
                if (i === 19) {
                    indexResp.done = true;
                    indexResp.continuation = new Buffer('1234');
                }
                siq.onSuccess(indexResp);
            }
        });
        
        it('should take multiple RpbIndexResp with term/key pairs and stream the response', function(done) {
            
            var count = 0;
            var timesCalled = 0;
            var callback = function(err, response) {
                timesCalled++;
                count += response.values.length;
                
                assert.equal(response.values[0].indexKey, 123);
                assert.equal(response.values[0].objectKey, 'object_key');
                
                
                if (response.done) {
                    assert.equal(timesCalled, 20);
                    assert.equal(count, 100);
                    assert.equal(response.continuation.toString('utf8'), '1234');
                    done();
                }
            };
            
            var siq = new SecondaryIndexQuery.Builder()
                    .withBucket('MyBucket')
                    .withIndexName('id_int')
                    .withCallback(callback)
                    .withStreaming(true)
                    .withReturnKeyAndIndex(true)
                    .withRange(0, 50)
                    .build();
            
            for (var i = 0; i < 20; i++) {
                var indexResp = new RpbIndexResp();    
                for (var j = 0; j < 5; j++) {
                    var pair = new RpbPair();
                    pair.key = new Buffer('123');
                    pair.value = new Buffer('object_key');
                    indexResp.results.push(pair);
                }
                if (i === 19) {
                    indexResp.done = true;
                    indexResp.continuation = new Buffer('1234');
                }
                siq.onSuccess(indexResp);
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
           
           var siq = new SecondaryIndexQuery.Builder()
                    .withBucket('MyBucket')
                    .withIndexName('id_bin')
                    .withCallback(callback)
                    .withStreaming(false)
                    .withReturnKeyAndIndex(true)
                    .withRange(0,50)
                    .build();
            
            siq.onRiakError(rpbErrorResp);
           
           
       });
        
    });
});
    
