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

var FetchValue = require('../../lib/commands/kv/fetchvalue');

var assert = require('assert');

describe('FetchValue', function() {
    describe('Build', function() {
        it('should build a RpbGetReq correctly', function(done) {
            
            var vclock = new Buffer(0);
            var fetchCommand = new FetchValue.Builder()
               .withBucketType('bucket_type')
               .withBucket('bucket_name')
               .withKey('key')
               .withR(3)
               .withPr(1)
               .withNotFoundOk(true)
               .withBasicQuorum(true)
               .withReturnDeletedVClock(true)
               .withHeadOnly(true)
               .withIfNotModified(vclock)
               .withTimeout(20000)
               .withCallback(function(){})
               .build();
       
            var protobuf = fetchCommand.protobuf;
            
            assert.equal(protobuf.getType().toString('utf8'), 'bucket_type');
            assert.equal(protobuf.getBucket().toString('utf8'), 'bucket_name');
            assert.equal(protobuf.getKey().toString('utf8'), 'key');
            assert.equal(protobuf.getR(), 3);
            assert.equal(protobuf.getPr(), 1);
            assert.equal(protobuf.getNotfoundOk(), true);
            assert.equal(protobuf.getBasicQuorum(), true);
            assert.equal(protobuf.getDeletedvclock(), true);
            assert.equal(protobuf.getHead(), true);
            assert(protobuf.getIfModified().toBuffer() !== null);
            assert.equal(protobuf.getTimeout(), 20000);
            done();
            
        });
        
    });
});