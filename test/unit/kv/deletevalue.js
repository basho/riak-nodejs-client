/**
 *
 * Copyright 2014-present Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

var rpb = require('../../../lib/protobuf/riakprotobuf');
var DeleteValue = require('../../../lib/commands/kv/deletevalue');
var RpbDelResp = rpb.getProtoFor('RpbDelResp');
var RpbErrorResp = rpb.getProtoFor('RpbErrorResp');

var assert = require('assert');
var crypto = require('crypto');

describe('DeleteValue', function() {
    describe('Build', function() {
        it('should build a RpbDelReq correctly', function(done) {
            var deleteValue = new DeleteValue.Builder()
               .withBucketType('bucket_type')
               .withBucket('bucket_name')
               .withKey('key')
               .withR(1)
               .withPr(2)
               .withW(3)
               .withPw(4)
               .withDw(5)
               .withRw(6)
               .withVClock(new Buffer('1234'))
               .withTimeout(20000)
               .withCallback(function(){})
               .build();

            var protobuf = deleteValue.constructPbRequest();

            assert.equal(protobuf.getType().toString('utf8'), 'bucket_type');
            assert.equal(protobuf.getBucket().toString('utf8'), 'bucket_name');
            assert.equal(protobuf.getKey().toString('utf8'), 'key');
            assert.equal(protobuf.getR(), 1);
            assert.equal(protobuf.getPr(), 2);
            assert.equal(protobuf.getW(), 3);
            assert.equal(protobuf.getPw(), 4);
            assert.equal(protobuf.getDw(), 5);
            assert.equal(protobuf.getRw(), 6);
            assert(protobuf.getVclock().toString('utf8'), '1234');
            assert.equal(protobuf.getTimeout(), 20000);
            done();
        });

        it('should build a RpbDelReq correctly with a binary key', function(done) {
            var binaryKey = crypto.randomBytes(128);
            var deleteValue = new DeleteValue.Builder()
               .withBucketType('bucket_type')
               .withBucket('bucket_name')
               .withKey(binaryKey)
               .withCallback(function(){})
               .build();
            var protobuf = deleteValue.constructPbRequest();
            var keyBuf = protobuf.getKey().toBuffer();
            assert(binaryKey.equals(keyBuf));
            done();
        });
        
        it('should take a RpbDelResp and call the users callback with the response', function(done) {
            // Riak doesn't actually return a RpbDelResp message - just the code. The core
            // will send null to the command onSuccess and the response sent to the user
            // is simply a boolean true.
            var callback = function(err, resp) {
                assert(resp === true);
                done();
            };

            var deleteValue = new DeleteValue.Builder()
                .withBucketType('bucket_type')
                .withBucket('bucket_name')
                .withKey('key')
                .withCallback(callback)
                .build();

            deleteValue.onSuccess(null);
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
            
            var deleteValue = new DeleteValue.Builder()
                .withBucketType('bucket_type')
                .withBucket('bucket_name')
                .withKey('key')
                .withCallback(callback)
                .build();
        
            deleteValue.onRiakError(rpbErrorResp);
        });
    });
});
