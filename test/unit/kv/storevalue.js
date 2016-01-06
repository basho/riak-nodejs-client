'use strict';

var rpb = require('../../../lib/protobuf/riakprotobuf');
var StoreValue = require('../../../lib/commands/kv/storevalue');
var RiakObject = require('../../../lib/commands/kv/riakobject');
var RpbPutResp = rpb.getProtoFor('RpbPutResp');
var RpbContent = rpb.getProtoFor('RpbContent');
var RpbPair = rpb.getProtoFor('RpbPair');
var RpbErrorResp = rpb.getProtoFor('RpbErrorResp');

var assert = require('assert');
var crypto = require('crypto');

describe('StoreValue', function() {
    describe('Build', function() {
        it('should build a RpbPutReq correctly', function(done) {
            var value = 'this is a value';
            var riakObject = new RiakObject();
            riakObject.setUserMeta([{key: 'metaKey1', value: 'metaValue1'}]);
            riakObject.addToIndex('email_bin','roach@basho.com');
            riakObject.setContentType('application/json');
            riakObject.setContentEncoding('gzip');
            riakObject.setValue('this is a value');
            riakObject.setLinks([{bucket: 'b', key: 'k', tag: 't'},
                {bucket: 'b', key: 'k2', tag: 't2'}]);
            
            var vclock = new Buffer(0);
            var storeCommand = new StoreValue.Builder()
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
               .withContent(riakObject)
               .withCallback(function(){})
               .build();
       
            var protobuf = storeCommand.constructPbRequest();
            
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

            var content = protobuf.getContent();
            assert.equal(content.getValue().toString('utf8'), value);
            assert.equal(content.getContentType().toString('utf8'), 'application/json');
            assert.equal(content.getContentEncoding().toString('utf8'), 'gzip');
            assert(content.getIndexes().length === 1);
            assert.equal(content.getIndexes()[0].key.toString('utf8'), 'email_bin');
            assert.equal(content.getIndexes()[0].value.toString('utf8'), 'roach@basho.com');
            assert(content.getUsermeta().length === 1);
            assert.equal(content.getUsermeta()[0].key.toString('utf8'), 'metaKey1');
            assert.equal(content.getUsermeta()[0].value.toString('utf8'), 'metaValue1');
            assert.equal(content.getLinks()[0].bucket.toString('utf8'), 'b');
            assert.equal(content.getLinks()[0].key.toString('utf8'), 'k');
            assert.equal(content.getLinks()[0].tag.toString('utf8'), 't');
            assert.equal(content.getLinks()[1].bucket.toString('utf8'), 'b');
            assert.equal(content.getLinks()[1].key.toString('utf8'), 'k2');
            assert.equal(content.getLinks()[1].tag.toString('utf8'), 't2');
            assert.equal(protobuf.getTimeout(), 20000);
            done();
        });

        it('should build a RpbPutReq correctly with a binary key', function(done) {
            var binaryKey = crypto.randomBytes(128);
            var cmd = new StoreValue.Builder()
               .withBucketType('bucket_type')
               .withBucket('bucket_name')
               .withKey(binaryKey)
               .withContent('test content')
               .withCallback(function(){})
               .build();
            var protobuf = cmd.constructPbRequest();
            var keyBuf = protobuf.getKey().toBuffer();
            assert(binaryKey.equals(keyBuf));
            done();
        });
        
        it('should require a bucket from options or RiakObject', function(done) {
            var b = new StoreValue.Builder()
                .withContent('test content')
                .withCallback(function(){});
            assert.throws(
                function () {
                    b.build();
                },
                function (err) {
                    assert.strictEqual(err.message, 'Must supply bucket directly or via a RiakObject');
                    return true;
                }
            );
            done();
        });

        it('should take the key, bucket, type and vclock from a RiakObject', function(done) {
            var value = 'this is a value';
            var riakObject = new RiakObject()
                    .setKey('key')
                    .setBucket('bucket_name')
                    .setBucketType('bucket_type')
                    .setVClock(new Buffer('1234'))
                    .setContentType('text/plain')
                    .setContentEncoding('utf8')
                    .setValue('this is a value');
            
            var storeCommand = new StoreValue.Builder()
                    .withContent(riakObject)
                    .withCallback(function(){})
                    .build();
            
            var protobuf = storeCommand.constructPbRequest();
            
            assert.equal(protobuf.getType().toString('utf8'), 'bucket_type');
            assert.equal(protobuf.getBucket().toString('utf8'), 'bucket_name');
            assert.equal(protobuf.getKey().toString('utf8'), 'key');
            assert.equal(protobuf.getVclock().toString('utf8'), '1234');

            var content = protobuf.getContent();
            assert.equal(content.getContentType().toString('utf8'), 'text/plain');
            assert.equal(content.getContentEncoding().toString('utf8'), 'utf8');
            assert.equal(content.getValue().toString('utf8'), 'this is a value');
            done();
        });
        
        it('should take a RpbPutResp and call the users callback with the response', function(done) {
            
            var rpbContent = new RpbContent();
            rpbContent.setValue(new Buffer('this is a value'));
            rpbContent.setContentType(new Buffer('application/json'));
            
            var pair = new RpbPair();
            pair.setKey(new Buffer('email_bin'));
            pair.setValue(new Buffer('roach@basho.com'));
            rpbContent.indexes.push(pair);
            
            pair = new RpbPair();
            pair.setKey(new Buffer('metaKey1'));
            pair.setValue(new Buffer('metaValue1'));
            rpbContent.usermeta.push(pair);
            
            var rpbPutResp = new RpbPutResp();
            rpbPutResp.setContent(rpbContent);
            rpbPutResp.setVclock(new Buffer('1234'));
            
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
                    assert.equal(riakObject.getVClock().toString('utf8'), '1234');
                    done();
                }
            };
            
            var storeCommand = new StoreValue.Builder()
               .withBucketType('bucket_type')
               .withBucket('bucket_name')
               .withKey('key')
               .withContent('dummy')
               .withCallback(callback)
               .build();
       
            storeCommand.onSuccess(rpbPutResp);
            
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
           
           var storeCommand = new StoreValue.Builder()
               .withBucketType('bucket_type')
               .withBucket('bucket_name')
               .withKey('key')
               .withContent('dummy')
               .withCallback(callback)
               .build();
       
            storeCommand.onRiakError(rpbErrorResp);
           
        });
        
    });
});
