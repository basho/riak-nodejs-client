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
var FetchPreflist = require('../../../lib/commands/kv/fetchpreflist');
var RiakNode = require('../../../lib/core/riaknode');
var RiakCluster = require('../../../lib/core/riakcluster');
var assert = require('assert');
var logger = require('winston');

var cb = function (err, resp) {
    assert(!err, err);
    assert.equal(resp.preflist.length, 3); // NB: since nval is 3
};

describe('FetchPreflist - Integration', function() {
    var cluster;
    this.timeout(10000);

    before(function(done) {
        var nodes = RiakNode.buildNodes(Test.nodeAddresses);
        cluster = new RiakCluster({ nodes: nodes});
        cluster.start();
        done();
    });

    it('Should fetch a preflist from Riak (default type)', function(done) {
        var fetch = new FetchPreflist.Builder()
                .withBucket(Test.bucketName)
                .withKey('my_key1')
                .withCallback(function (err, resp) { cb(err, resp); done(); })
                .build();
        
        cluster.execute(fetch);
    });
    
    it('Should fetch a preflist for a non-default bucket-type from Riak', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.preflist.length, 3); // NB: since nval is 3
            done();
        };
        
        var fetch = new FetchPreflist.Builder()
                .withBucket(Test.bucketName)
                .withBucketType(Test.bucketType)
                .withKey('my_key1')
                .withCallback(function (err, resp) { cb(err, resp); done(); })
                .build();
        
        cluster.execute(fetch);
    });
});
