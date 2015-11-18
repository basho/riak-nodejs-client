'use strict';

var FetchCounter = require('../../../lib/commands/crdt/fetchcounter');
var DtFetchResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('DtFetchResp');
var DtValue = require('../../../lib/protobuf/riakprotobuf').getProtoFor('DtValue');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');

var assert = require('assert');

describe('FetchCounter', function() {
    describe('Build', function() {
        it('should build a DtFetchReq correctly', function(done) {
            var fetch = new FetchCounter.Builder()
                .withBucketType('counters')
                .withBucket('myBucket')
                .withKey('counter_1')
                .withCallback(function(){})
                .withR(1)
                .withPr(2)
                .withNotFoundOk(true)
                .withBasicQuorum(true)
                .withTimeout(20000)
                .build();
        
            var protobuf = fetch.constructPbRequest();
            
            assert.equal(protobuf.getType().toString('utf8'), 'counters');
            assert.equal(protobuf.getBucket().toString('utf8'), 'myBucket');
            assert.equal(protobuf.getKey().toString('utf8'), 'counter_1');
            assert.equal(protobuf.getR(), 1);
            assert.equal(protobuf.getPr(), 2);
            assert.equal(protobuf.getNotfoundOk(), true);
            assert.equal(protobuf.getBasicQuorum(), true);
            assert.equal(protobuf.getTimeout(), 20000);
            done();
        });
        
        it('should take a DtFetchResp and call the users callback with the response', function(done) {
            var dtFetchResp = new DtFetchResp();
            var dtValue = new DtValue();
            dtValue.setCounterValue(42);
            dtFetchResp.value = dtValue;
            
            var callback = function(err, response) {
                if (response) {
                    assert.equal(response.counterValue, 42);
                    done();
                }
            };
            
            var fetch = new FetchCounter.Builder()
                .withBucketType('counters')
                .withBucket('myBucket')
                .withKey('counter_1')
                .withCallback(callback)
                .build();
        
            fetch.onSuccess(dtFetchResp);
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
           
            var fetch = new FetchCounter.Builder()
                .withBucketType('counters')
                .withBucket('myBucket')
                .withKey('counter_1')
                .withCallback(callback)
                .build();
        
            fetch.onRiakError(rpbErrorResp);
        });
        
    });
});
