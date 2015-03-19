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

var StoreValue = require('../../lib/commands/kv/storevalue');
var RiakMeta = require('../../commands/kv/riakmeta');

var assert = require('assert');

describe('StoreValue', function() {
    describe('Build', function() {
        it('should build a RpbPutReq correctly', function(done) {
            
            var value = 'this is a value';
            var meta = new RiakMeta();
            meta.setUserMeta([{key: 'metaKey1', value: 'metaValue1'}]);
            meta.addToIndex('email_bin','roach@basho.com');
            meta.setContentType('application/json');
            
            
            var vclock = new Buffer(0);
            var fetchCommand = new FetchValue.Builder()
               .withBucketType('bucket_type')
               .withBucket('bucket_name')
               .withKey('key')
               .withW(3)
               .withPw(1)
               .withDw(2)
               .withVClock(vclock)
               .withReturnHead(true)
               .withReturnBody(true)
               .withIfNotModified(true)
               .withIfNoneMatch(true)
               .withTimeout(20000)
               .withContent(value, meta)
               .withCallback(function(){})
               .build();
       
            var protobuf = fetchCommand.constructPbRequest();
            
            assert.equal(protobuf.getType().toString('utf8'), 'bucket_type');
            assert.equal(protobuf.getBucket().toString('utf8'), 'bucket_name');
            assert.equal(protobuf.getKey().toString('utf8'), 'key');
            assert.equal(protobuf.getW(), 3);
            assert.equal(protobuf.getPw(), 1);
            assert.equal(protobuf.getDw(), 2);
            assert(protobuf.getVclock() !== null);
            assert.equal(protobuf.getReturnHead(), true);
            assert.equal(protobuf.getIfNotModified(), true);
            assert.equal(protobuf.getIfNoneMatch(), true);
            assert.equal(protobuf.getContent().getValue().toString('utf8'), value);
            assert.equal(protobuf.getContent().getContentType().toString('utf8'), 'application/json');
            assert(protobuf.getIndexes().length === 1);
            assert.equals(protobuf.getIndexes()[0].key.toString('utf8'), 'email_bin');
            assert.equals(protobuf.getIndexes()[0].value.toString('utf8'), 'roach@basho.com');
            assert(protobuf.getUsermeta().length === 1);
            assert.equals(protobuf.getUsermeta()[0].key.toString('utf8'), 'metaKey1');
            assert.equals(protobuf.getUsermeta()[0].value.toString('utf8'), 'metaValue1');
            assert.equal(protobuf.getTimeout(), 20000);
            done();
            
        });
        
    });
});