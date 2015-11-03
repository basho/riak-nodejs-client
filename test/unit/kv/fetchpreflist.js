'use strict';

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

var rpb = require('../../../lib/protobuf/riakprotobuf');
var FetchPreflist = require('../../../lib/commands/kv/fetchpreflist');
var RpbGetBucketKeyPreflistReq = rpb.getProtoFor('RpbGetBucketKeyPreflistReq');
var RpbGetBucketKeyPreflistResp = rpb.getProtoFor('RpbGetBucketKeyPreflistResp');
var RpbBucketKeyPreflistItem = rpb.getProtoFor('RpbBucketKeyPreflistItem');
var RpbErrorResp = rpb.getProtoFor('RpbErrorResp');

var assert = require('assert');
var crypto = require('crypto');

var bucketType = 'bucket_type_name';
var bucket = 'bucket_name';
var key = 'key_name';

function getRandom() {
  return Math.floor(Math.random() * Math.pow(2, 32));
}

describe('FetchPreflist', function() {

    describe('Build', function() {

        it('should build a RpbGetBucketKeyPreflistReq correctly', function(done) {
            var fetchCommand = new FetchPreflist.Builder()
               .withBucketType(bucketType)
               .withBucket(bucket)
               .withKey(key)
               .withCallback(function(){})
               .build();
       
            var protobuf = fetchCommand.constructPbRequest();
            
            assert.equal(protobuf.getType().toString('utf8'), bucketType);
            assert.equal(protobuf.getBucket().toString('utf8'), bucket);
            assert.equal(protobuf.getKey().toString('utf8'), key);

            done();
        });

        it('should build a RpbGetBucketKeyPreflistReq correctly with a binary key', function(done) {
            var binaryKey = crypto.randomBytes(128);
            var cmd = new FetchPreflist.Builder()
               .withBucketType('bucket_type')
               .withBucket('bucket_name')
               .withKey(binaryKey)
               .withCallback(function(){})
               .build();
            var protobuf = cmd.constructPbRequest();
            var keyBuf = protobuf.getKey().toBuffer();
            assert(binaryKey.equals(keyBuf));
            done();
        });
        
        it('should take a RpbGetBucketKeyPreflistResp and call the users callback with the response', function(done) {
            var partitionId = getRandom();
            var node_name = 'node-foo';

            var rpbItem = new RpbBucketKeyPreflistItem();
            rpbItem.setPartition(partitionId);
            rpbItem.setNode(new Buffer(node_name));
            rpbItem.setPrimary(true);
            
            var rpbGetResp = new RpbGetBucketKeyPreflistResp();
            rpbGetResp.preflist.push(rpbItem);
            
            var callback = function(err, response) {
                if (response) {
                    assert.equal(response.preflist.length, 1);
                    var preflistItem = response.preflist.shift();
                    assert.equal(preflistItem.partition, partitionId);
                    assert.equal(preflistItem.node, node_name);
                    assert.equal(preflistItem.primary, true);
                    done();
                }
            };
            
            var fetchCommand = new FetchPreflist.Builder()
                .withBucketType(bucketType)
                .withBucket(bucket)
                .withKey(key)
                .withCallback(callback)
                .build();

            fetchCommand.onSuccess(rpbGetResp);
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
           
           var fetchCommand = new FetchPreflist.Builder()
               .withBucketType('bucket_type')
               .withBucket('bucket_name')
               .withKey('key')
               .withCallback(callback)
               .build();
       
            fetchCommand.onRiakError(rpbErrorResp);
       });

    });
});
