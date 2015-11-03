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

var Test = require('../testparams');
var StoreValue = require('../../../lib/commands/kv/storevalue');
var RiakObject = require('../../../lib/commands/kv/riakobject');
var SecondaryIndexQuery = require('../../../lib/commands/kv/secondaryindexquery');
var RiakNode = require('../../../lib/core/riaknode');
var RiakCluster = require('../../../lib/core/riakcluster');
var assert = require('assert');

describe('Secondary Index Query - Integration', function() {
   
    var cluster;
    this.timeout(10000);
    
    before(function(done) {
       
        var nodes = RiakNode.buildNodes(Test.nodeAddresses);
        cluster = new RiakCluster({ nodes: nodes});
        cluster.start();
        
        var count = 0;
        var storeCb = function(err, response) {

            count++;
            if (count === 100) {
                done();
            }
        };
    
    
        for (var i = 0; i < 25; i++) {
            var ro = new RiakObject();
            ro.addToIndex('id_int', i);
            ro.setValue('this is a value');
            var store = new StoreValue.Builder()
                    .withBucketType(Test.bucketType)
                    .withBucket(Test.bucketName)
                    .withContent(ro)
                    .withKey('key' + i)
                    .withCallback(storeCb)
                    .build();

            cluster.execute(store);
            
            ro = new RiakObject();
            ro.addToIndex('email_bin', 'email' + i);
            ro.setValue('this is a value');
            store = new StoreValue.Builder()
                    .withBucketType(Test.bucketType)
                    .withBucket(Test.bucketName)
                    .withContent(ro)
                    .withKey('key_' + i)
                    .withCallback(storeCb)
                    .build();

            cluster.execute(store);
            
            ro = new RiakObject();
            ro.addToIndex('id_int', i);
            ro.setValue('this is a value');
            store = new StoreValue.Builder()
                    .withBucketType(Test.bucketType)
                    .withBucket(Test.bucketName)
                    .withContent(ro)
                    .withKey('key' + i)
                    .withCallback(storeCb)
                    .build();

            cluster.execute(store);
            
            ro = new RiakObject();
            ro.addToIndex('email_bin', 'email' + i);
            ro.setValue('this is a value');
            store = new StoreValue.Builder()
                    .withBucketType(Test.bucketType)
                    .withBucket(Test.bucketName)
                    .withContent(ro)
                    .withKey('key_' + i)
                    .withCallback(storeCb)
                    .build();

            cluster.execute(store);
            
        }

    });
    
    after(function(done) {
        Test.cleanBucket(cluster, 'default', Test.bucketName, function() { 
            Test.cleanBucket(cluster, Test.bucketType, Test.bucketName, function() {
                cluster.on('stateChange', function(state) { if (state === RiakCluster.State.SHUTDOWN) { done(); } });
                cluster.stop();
            });
        });
        
   });
   
   it('Should perform a _int query against the default type', function(done) {
      
        var count = 0;
        var callback = function(err, response) {
            assert(!err, err);
            count += response.values.length;
            if (response.done) {
                assert.equal(count, 25);
                done();
            }
        };
        
        var siq = new SecondaryIndexQuery.Builder()
					.withBucket(Test.bucketName)
                    .withBucketType(Test.bucketType)
					.withIndexName('id_int')
					.withRange(0,10000)
					.withCallback(callback)
					.withReturnKeyAndIndex(true)
					.build();

		cluster.execute(siq);
       
   });
   
   it('Should perform a _int query against a non-default type', function(done) {
      
        var count = 0;
        var callback = function(err, response) {
            assert(!err, err);
            count += response.values.length;
            if (response.done) {
                assert.equal(count, 25);
                done();
            }
        };
        
        var siq = new SecondaryIndexQuery.Builder()
					.withBucket(Test.bucketName)
                    .withBucketType(Test.bucketType)
					.withIndexName('id_int')
					.withRange(0,10000)
					.withCallback(callback)
					.withReturnKeyAndIndex(true)
					.build();

		cluster.execute(siq);
       
   });
   
   it('Should perform a _bin query against the default type', function(done) {
      
        var count = 0;
        var callback = function(err, response) {
            assert(!err, err);
            count += response.values.length;
            if (response.done) {
                assert.equal(count, 25);
                done();
            }
        };
        
        var siq = new SecondaryIndexQuery.Builder()
					.withBucket(Test.bucketName)
                    .withBucketType(Test.bucketType)
					.withIndexName('email_bin')
					.withRange('a','z')
					.withCallback(callback)
					.withReturnKeyAndIndex(true)
					.build();

		cluster.execute(siq);
       
   });
   
   it('Should perform a _bin query against a non-default type', function(done) {
      
        var count = 0;
        var callback = function(err, response) {
            assert(!err, err);
            count += response.values.length;
            if (response.done) {
                assert.equal(count, 25);
                done();
            }
        };
        
        var siq = new SecondaryIndexQuery.Builder()
					.withBucket(Test.bucketName)
                    .withBucketType(Test.bucketType)
					.withIndexName('email_bin')
					.withRange('a','z')
					.withCallback(callback)
					.withReturnKeyAndIndex(true)
					.build();

		cluster.execute(siq);
       
   });
   
   it('Should set a coninuation on a paginated query', function(done) {
      
       var count = 0;
        var callback = function(err, response) {
            assert(!err, err);
            count += response.values.length;
            if (response.done) {
                assert.equal(count, 10);
                assert(response.continuation);
                done();
            }
        };
        
        var siq = new SecondaryIndexQuery.Builder()
					.withBucket(Test.bucketName)
                    .withBucketType(Test.bucketType)
					.withIndexName('email_bin')
					.withRange('a','z')
                    .withMaxResults(10)
					.withCallback(callback)
					.withReturnKeyAndIndex(true)
					.build();

		cluster.execute(siq);
       
   });
    
});

