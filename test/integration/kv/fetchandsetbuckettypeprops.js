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
var StoreBucketTypeProps = require('../../../lib/commands/kv/storebuckettypeprops');
var FetchBucketTypeProps = require('../../../lib/commands/kv/fetchbuckettypeprops');
var RiakNode = require('../../../lib/core/riaknode');
var RiakCluster = require('../../../lib/core/riakcluster');
var assert = require('assert');

var bucketName = Test.bucketName + '_sp';

describe('Store and Fetch Bucket props - Integration', function() {
   
    this.timeout(10000);

    var cluster;
    before(function(done) {
        var nodes = RiakNode.buildNodes(Test.nodeAddresses);
        cluster = new RiakCluster({ nodes: nodes});
        cluster.start();
        done();
    });
    
    after(function(done) {
        var sc = function (state) {
            if (state === RiakCluster.State.SHUTDOWN) {
                done();
            }
        };
        cluster.on('stateChange', sc);
        cluster.stop();
    });
    
    describe('StoreBucketTypeProps', function() {
        it('Should NOT store props for the default bucket type', function(done) {
            var callback = function(err, resp) {
                assert(err, err);
                done();
            };
            var store = new StoreBucketTypeProps.Builder()
                    .withAllowMult(true)
                    .withCallback(callback)
                    .build();
            cluster.execute(store);
        });
    
        it('Should store bucket props for a non-default bucket type', function(done) {
            var cb2 = function(err, resp) {
                assert(!err, err);
                done();
            };
            var cb1 = function(err, resp) {
                assert(!err, err);
                var store = new StoreBucketTypeProps.Builder()
                        .withBucketType(Test.bucketType)
                        .withAllowMult(true)
                        .withCallback(cb2)
                        .build();
                cluster.execute(store);
            };
            var store = new StoreBucketTypeProps.Builder()
                    .withBucketType(Test.bucketType)
                    .withAllowMult(false)
                    .withCallback(cb1)
                    .build();
            cluster.execute(store);
        });
    });
    
    describe('FetchBucketTypeProps', function() {
        it('Should fetch props for the default bucket type', function(done) {
            var callback = function(err, resp) {
                assert(!err, err);
                assert(resp.nVal);
                done();
            };
            var fetch = new FetchBucketTypeProps.Builder()
                .withCallback(callback)
                .build();
            cluster.execute(fetch);
        });
        
        it('Should fetch props for a non-default bucket type', function(done) {
            var callback = function(err, resp) {
                assert(!err, err);
                assert(resp.nVal);
                done();
            };
            var fetch = new FetchBucketTypeProps.Builder()
                .withBucketType(Test.bucketType)
                .withCallback(callback)
                .build();
            cluster.execute(fetch);
        });
    });
});
