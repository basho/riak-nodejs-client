'use strict';

var Test = require('../testparams');
var StoreValue = require('../../../lib/commands/kv/storevalue');
var RiakObject = require('../../../lib/commands/kv/riakobject');
var RiakNode = require('../../../lib/core/riaknode');
var RiakCluster = require('../../../lib/core/riakcluster');
var assert = require('assert');

describe('StoreValue - Integration', function() {
   
    this.timeout(10000);
   
    var cluster;
    
    before(function(done) {
        var nodes = RiakNode.buildNodes(Test.nodeAddresses);
        cluster = new RiakCluster({ nodes: nodes});
        cluster.start();
        cluster.start(function (err, rslt) {
            assert(!err, err);
            done();
        });
    });
   
    it('Should store a (String) value in Riak (default type) and return it', function(done) {
            
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.generatedKey, null);
            assert(resp.vclock);
            assert.equal(resp.values.length, 1);
            assert.equal(resp.values[0].getValue().toString('utf8'), 'this is a value in Riak');
            done();
            
        };
      
        var store = new StoreValue.Builder()
               .withBucket(Test.bucketName + '_sv')
               .withKey('my_key1')
               .withContent('this is a value in Riak')
               .withCallback(callback)
               .withReturnBody(true)
               .build();
       
       cluster.execute(store);
       
   });
   
   it('Should store a JS object as JSON in Riak (default type) and return it', function(done) {
       
       var myObject = { field1: 'field1_value', field2: 'field2_value', field3: 7 };
       
       var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.generatedKey, null);
            assert(resp.vclock);
            assert.equal(resp.values.length, 1);
            
            var returnedObj = resp.values[0].getValue();
            
            assert.equal(returnedObj.field1, 'field1_value');
            assert.equal(returnedObj.field2, 'field2_value');
            assert.equal(returnedObj.field3, 7);
            done();
            
        };
        
        var store = new StoreValue.Builder()
               .withBucket(Test.bucketName + '_sv')
               .withKey('my_key2')
               .withContent(myObject)
               .withCallback(callback)
               .withReturnBody(true, true)
               .build();
       
       cluster.execute(store);
       
   });
   
   it('Should store a buffer in Riak (default type) and return it', function(done) {
       
       var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.generatedKey, null);
            assert(resp.vclock);
            assert.equal(resp.values.length, 1);
            
            assert.equal(resp.values[0].getValue().toString('utf8'), 'some content');
            done();
            
        };
        
        var store = new StoreValue.Builder()
               .withBucket(Test.bucketName + '_sv')
               .withKey('my_key3')
               .withContent(new Buffer('some content'))
               .withCallback(callback)
               .withReturnBody(true)
               .build();
       
       cluster.execute(store);
       
   });
   
   it('Should store an object in a non-default bucket-type and return it', function(done) {
      
       var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.generatedKey, null);
            assert(resp.vclock);
            assert.equal(resp.values.length, 1);
            assert.equal(resp.values[0].getValue().toString('utf8'), 'this is a value in Riak');
            done();
            
        };
      
        var store = new StoreValue.Builder()
               .withBucket(Test.bucketName + '_sv')
               .withBucketType(Test.bucketType)
               .withKey('my_key1')
               .withContent('this is a value in Riak')
               .withCallback(callback)
               .withReturnBody(true)
               .build();
       
       cluster.execute(store);
       
   });
   
   it('Should store an object and not return it (returnBody=false)', function(done) {
      
       var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.generatedKey, null);
            assert(!resp.vclock);
            assert.equal(resp.values.length, 0);
            done();
            
        };
      
        var store = new StoreValue.Builder()
               .withBucket(Test.bucketName + '_sv')
               .withKey('my_key4')
               .withContent('this is a value in Riak')
               .withCallback(callback)
               .build();
       
       cluster.execute(store);
       
   });
   
   it('Should store a value and generate a key, returning it', function(done) {
      
        var callback = function(err, resp) {
             assert(!err, err);
             assert(resp.generatedKey);
             done();

         };

         var store = new StoreValue.Builder()
                .withBucket(Test.bucketName + '_sv')
                .withContent('this is a value in Riak')
                .withCallback(callback)
                .build();
       
        cluster.execute(store);
       
   });
   
   it('Should store a RiakObject and generate a key, returning it', function(done) {
        var ro = new RiakObject();
        ro.setBucket(Test.bucketName + '_sv');
        ro.setContentType('text/plain');
        ro.setContentEncoding('utf8');
        ro.setValue('this is a string');

        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp.generatedKey);

            var obj = resp.values.shift();

            var val = obj.getValue().toString('utf8');
            assert.equal(val, 'this is a string');

            var contentType = obj.getContentType().toString('utf8');
            assert.equal(contentType, 'text/plain');

            var contentEncoding = obj.getContentEncoding().toString('utf8');
            assert.equal(contentEncoding, 'utf8');

            done();
        };

        var store = new StoreValue.Builder()
            .withContent(ro)
            .withCallback(callback)
            .withReturnBody(true)
            .build();

        cluster.execute(store);
   });
   
   after(function(done) {
        Test.cleanBucket(cluster, 'default', Test.bucketName + '_sv', function() { 
            Test.cleanBucket(cluster, Test.bucketType, Test.bucketName + '_sv', function() {
                cluster.on('stateChange', function(state) { if (state === RiakCluster.State.SHUTDOWN) { done();} });
                cluster.stop();
            });
        });
   });
   
});
