/*
 * Copyright 2015 Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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