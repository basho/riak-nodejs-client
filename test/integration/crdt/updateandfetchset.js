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
var UpdateSet = require('../../../lib/commands/crdt/updateset');
var FetchSet = require('../../../lib/commands/crdt/fetchset');
var RiakNode = require('../../../lib/core/riaknode');
var RiakCluster = require('../../../lib/core/riakcluster');
var assert = require('assert');

describe('Update and Fetch Set - Integration', function() {
    
    var cluster;
    this.timeout(10000);
    var context;
    
    before(function(done) {
        var nodes = RiakNode.buildNodes(Test.nodeAddresses);
        cluster = new RiakCluster({ nodes: nodes});
        cluster.start();
        
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.values.length, 2);
            assert(resp.context);
            context = resp.context;
            done();
        };
        
        // This ... tests updating a new set
        var update = new UpdateSet.Builder()
                .withBucketType(Test.setBucketType)
                .withBucket(Test.bucketName)
                .withKey('set_1')
                .withAdditions(['this', 'that'])
                .withCallback(callback)
                .build();
        
        cluster.execute(update);
        
    });
    
    after(function(done) {
        Test.cleanBucket(cluster, Test.setBucketType, Test.bucketName, function() {
            cluster.on('stateChange', function(state) { if (state === RiakCluster.State.SHUTDOWN) { done();} });
            cluster.stop();
        });
    });
    
    it('Should fetch a set', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.values.length, 2);
            assert(resp.context);
            done();
        };
        var fetch = new FetchSet.Builder()
                .withBucketType(Test.setBucketType)
                .withBucket(Test.bucketName)
                .withKey('set_1')
                .withCallback(callback)
                .build();
        cluster.execute(fetch);
    });
    
    it('Should report isNotFound if a set does not exist', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp.notFound);
            assert(resp.isNotFound);
            done();
        };
        var fetch = new FetchSet.Builder()
                .withBucketType(Test.setBucketType)
                .withBucket(Test.bucketName)
                .withKey('set_notFound')
                .withCallback(callback)
                .build();
        cluster.execute(fetch);
    });
    
    it('Should remove from a set', function(done) {
       
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.values.length, 1);
            done();
        };
        
        // This ... tests updating a new set
        var update = new UpdateSet.Builder()
                .withBucketType(Test.setBucketType)
                .withBucket(Test.bucketName)
                .withKey('set_1')
                .withRemovals(['this'])
                .withContext(context)
                .withCallback(callback)
                .build();
        
        cluster.execute(update);
        
    });

});
