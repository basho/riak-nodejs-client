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
var RpbGetResp = require('../../lib/protobuf/riakprotobuf').getProtoFor('RpbGetResp');
var RpbContent = require('../../lib/protobuf/riakprotobuf').getProtoFor('RpbContent');
var RpbPair = require('../../lib/protobuf/riakprotobuf').getProtoFor('RpbPair');
var RpbLink = require('../../lib/protobuf/riakprotobuf').getProtoFor('RpbLink');
var RpbErrorResp = require('../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');

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
       
            var protobuf = fetchCommand.constructPbRequest();
            
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
        
        it('should take a RpbGetResp and call the users callback with the response', function(done) {
            
            var rpbContent = new RpbContent();
            rpbContent.setValue(new Buffer('this is a value'));
            rpbContent.setContentType(new Buffer('application/json'));
            rpbContent.setVtag(new Buffer('4lfAJ4HnZthG494VIkdjIb'));
            
            var pair = new RpbPair();
            pair.setKey(new Buffer('email_bin'));
            pair.setValue(new Buffer('roach@basho.com'));
            rpbContent.indexes.push(pair);
            
            pair = new RpbPair();
            pair.setKey(new Buffer('metaKey1'));
            pair.setValue(new Buffer('metaValue1'));
            rpbContent.usermeta.push(pair);
            
            var link = new RpbLink();
            link.setBucket(new Buffer('b'));
            link.setKey(new Buffer('k'));
            link.setTag(new Buffer('t'));
            rpbContent.links.push(link);
            link = new RpbLink();
            link.setBucket(new Buffer('b'));
            link.setKey(new Buffer('k2'));
            link.setTag(new Buffer('t2'));
            rpbContent.links.push(link);
            
            var rpbGetResp = new RpbGetResp();
            rpbGetResp.setContent(rpbContent);
            rpbGetResp.setVclock(new Buffer('1234'));
            
            var callback = function(err, response) {
                if (response) {
                    assert.equal(response.values.length, 1);
                    var riakObject = response.values[0];
                    assert.equal(riakObject.getBucketType(), 'bucket_type');
                    assert.equal(riakObject.getBucket(), 'bucket_name');
                    assert.equal(riakObject.getKey(), 'key');
                    assert.equal(riakObject.getContentType(), 'application/json');
                    assert.equal(riakObject.hasIndexes(), true);
                    assert.equal(riakObject.getIndex('email_bin')[0], 'roach@basho.com');
                    assert.equal(riakObject.hasUserMeta(), true);
                    assert.equal(riakObject.getUserMeta()[0].key, 'metaKey1');
                    assert.equal(riakObject.getUserMeta()[0].value, 'metaValue1');
                    assert.equal(riakObject.getLinks()[0].bucket, 'b');
                    assert.equal(riakObject.getLinks()[0].key, 'k');
                    assert.equal(riakObject.getLinks()[0].tag, 't');
                    assert.equal(riakObject.getLinks()[1].bucket, 'b');
                    assert.equal(riakObject.getLinks()[1].key, 'k2');
                    assert.equal(riakObject.getLinks()[1].tag, 't2');
                    assert.equal(riakObject.getVClock().toString('utf8'), '1234');
                    assert.equal(riakObject.getVTag().toString('utf8'), '4lfAJ4HnZthG494VIkdjIb');
                    done();
                }
            };
            
            var fetchCommand = new FetchValue.Builder()
               .withBucketType('bucket_type')
               .withBucket('bucket_name')
               .withKey('key')
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
           
           var fetchCommand = new FetchValue.Builder()
               .withBucketType('bucket_type')
               .withBucket('bucket_name')
               .withKey('key')
               .withCallback(callback)
               .build();
       
            fetchCommand.onRiakError(rpbErrorResp);
           
           
       });
       
    });
});
