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

var assert = require('assert');

var Test = require('../testparams');
var StoreValue = require('../../../lib/commands/kv/storevalue');
var FetchValue = require('../../../lib/commands/kv/fetchvalue');

describe('FetchValue - Integration', function() {
    var cluster;
    before(function(done) {
        cluster = Test.buildCluster(function (err, rslt) {
            assert(!err, err);

            var count = 0;
            var cb = function(err, resp) {
                assert(!err, err);
                count++;
                if (count === 5) {
                    done();
                }
            };

            var store = new StoreValue.Builder()
                .withBucket(Test.bucketName)
                .withKey('my_key1')
                .withContent('this is a value in Riak')
                .withCallback(cb)
                .build();
            cluster.execute(store);

            var myObject = { field1: 'field1_value', field2: 'field2_value', field3: 7 };
            store = new StoreValue.Builder()
                    .withBucket(Test.bucketName)
                    .withKey('my_key2')
                    .withContent(myObject)
                    .withCallback(cb)
                    .build();
            cluster.execute(store);

            for (var i = 0; i < 2; i++) {
                store = new StoreValue.Builder()
                    .withBucket(Test.bucketName)
                    .withBucketType(Test.bucketType)
                    .withKey('my_key2')
                    .withContent('this is a value in Riak')
                    .withCallback(cb)
                    .build();
                cluster.execute(store);
            }

            store = new StoreValue.Builder()
                    .withBucket(Test.bucketName)
                    .withBucketType(Test.bucketType)
                    .withKey('my_key1')
                    .withContent('this is a value in Riak')
                    .withCallback(cb)
                    .build();
            cluster.execute(store);
        });
    });
    
    after(function(done) {
        Test.cleanBucket(cluster, 'default', Test.bucketName, function() { 
            Test.cleanBucket(cluster, Test.bucketType, Test.bucketName, function() {
                cluster.stop(function (err, rslt) {
                    assert(!err, err);
                    done();
                });
            });
        });
   });

    it('Should fetch a value from Riak (default type)', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.values.length, 1);
            assert.equal(resp.values[0].getValue().toString('utf8'), 'this is a value in Riak');
            assert.equal(resp.isNotFound, false);
            done();
        };
        
        var fetch = new FetchValue.Builder()
                .withBucket(Test.bucketName)
                .withKey('my_key1')
                .withCallback(callback)
                .build();
        
        cluster.execute(fetch);
    });
    
    it('Should fetch a value in a non-default bucket-type from Riak', function(done) {
       
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.values.length, 1);
            assert.equal(resp.isNotFound, false);
            assert.equal(resp.values[0].getValue().toString('utf8'), 'this is a value in Riak');
            done();
        };
        
        var fetch = new FetchValue.Builder()
                .withBucket(Test.bucketName)
                .withBucketType(Test.bucketType)
                .withKey('my_key1')
                .withCallback(callback)
                .build();
        
        cluster.execute(fetch);
        
    });
    
    it('Should convert value from JSON if specified', function(done) {
       
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.values.length, 1);
            var value = resp.values[0].getValue();
            assert.equal(value.field1, 'field1_value' );
            assert.equal(value.field2, 'field2_value' );
            assert.equal(value.field3, 7 );

            assert.equal(resp.isNotFound, false);
            done();
        };
        
        var fetch = new FetchValue.Builder()
                .withBucket(Test.bucketName)
                .withKey('my_key2')
                .withConvertValueToJs(true)
                .withCallback(callback)
                .build();
        
        cluster.execute(fetch);
        
    });
    
    it('Should report not-found when value doesn\'t exist', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.values.length, 0);
            assert.equal(resp.isNotFound, true);
            done();
        };
        
        var fetch = new FetchValue.Builder()
                .withBucket(Test.bucketName)
                .withKey('no_key')
                .withCallback(callback)
                .build();
        
        cluster.execute(fetch);
    });
    
    
    
    
    
    it('Should return siblings if no conflict resolver', function(done) {
       
       var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.values.length, 2);
            assert.equal(resp.isNotFound, false);
            assert.equal(resp.values[0].getValue().toString('utf8'), 'this is a value in Riak');
            assert.equal(resp.values[1].getValue().toString('utf8'), 'this is a value in Riak');

            done();
        };
        
        var fetch = new FetchValue.Builder()
                .withBucket(Test.bucketName)
                .withBucketType(Test.bucketType)
                .withKey('my_key2')
                .withCallback(callback)
                .build();
        
        cluster.execute(fetch);
        
        
    });
    
    it('Should resolve siblings if conflict resolver provided', function(done) {
       
        var cf = function(objects) {
          
            assert.equal(objects.length, 2);
            return objects[0];
            
        };
        
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.values.length, 1);
            assert.equal(resp.isNotFound, false);
            assert.equal(resp.values[0].getValue().toString('utf8'), 'this is a value in Riak');

            done();
        };
        
        var fetch = new FetchValue.Builder()
                .withBucket(Test.bucketName)
                .withBucketType(Test.bucketType)
                .withKey('my_key2')
                .withCallback(callback)
                .withConflictResolver(cf)
                .build();
        
        cluster.execute(fetch);
        
        
    });

});
