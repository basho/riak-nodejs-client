'use strict';

var UpdateCounter = require('../../../lib/commands/crdt/updatecounter');
var DtUpdateResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('DtUpdateResp');
var DtValue = require('../../../lib/protobuf/riakprotobuf').getProtoFor('DtValue');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');

var assert = require('assert');

describe('UpdateCounter', function() {
    describe('Build', function() {
        it('should build a DtUpdateReq correctly', function(done) {
            
            var update = new UpdateCounter.Builder()
                .withBucketType('counters')
                .withBucket('myBucket')
                .withKey('counter_1')
                .withIncrement(100)
                .withCallback(function(){})
                .withW(3)
                .withPw(1)
                .withDw(2)
                .withReturnBody(true)
                .withTimeout(20000)
                .build();
        
            var protobuf = update.constructPbRequest();
            
            assert.equal(protobuf.getType().toString('utf8'), 'counters');
            assert.equal(protobuf.getBucket().toString('utf8'), 'myBucket');
            assert.equal(protobuf.getKey().toString('utf8'), 'counter_1');
            assert.equal(protobuf.getW(), 3);
            assert.equal(protobuf.getPw(), 1);
            assert.equal(protobuf.getDw(), 2);
            assert.equal(protobuf.getReturnBody(), true);
            assert.equal(protobuf.op.counter_op.increment, 100);
            assert.equal(protobuf.getTimeout(), 20000);
            done();

            
        });
        
        it('should take a DtUpdateResp and call the users callback with the response', function(done) {
            var dtUpdateResp = new DtUpdateResp();
            dtUpdateResp.setCounterValue(42);
            dtUpdateResp.setKey(new Buffer('NewGeneratedKey'));

            var callback = function(err, response) {
                if (response) {
                    assert.equal(response.counterValue, 42);
                    assert.equal(response.generatedKey, 'NewGeneratedKey');
                    done();
                }
            };

            var update = new UpdateCounter.Builder()
                .withBucketType('counters')
                .withBucket('myBucket')
                .withKey('counter_1')
                .withIncrement(100)
                .withCallback(callback)
                .build();
            update.onSuccess(dtUpdateResp);
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
            
            var update = new UpdateCounter.Builder()
                .withBucketType('counters')
                .withBucket('myBucket')
                .withKey('counter_1')
                .withIncrement(100)
                .withCallback(callback)
                .build();
        
            update.onRiakError(rpbErrorResp);
        });
    });
});

