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
var ListBuckets = require('../../../lib/commands/kv/listbuckets');
var StoreValue = require('../../../lib/commands/kv/storevalue');

describe('ListBuckets - Integration', function() {
    var cluster;
    before(function(done) {
        cluster = Test.buildCluster(function (err, rslt) {
            assert(!err, err);
            
            var count = 0;
            var cb = function(err, resp) {
                assert(!err, err);
                count++;
                if (count === 10) {
                    done();
                }
            };
            
            for (var i = 0; i < 5; i++) {
                // Will create buckets
                var bucket = Test.bucketName + '_lb' + i;
                var store = new StoreValue.Builder()
                        .withBucket(bucket)
                        .withContent('value')
                        .withCallback(cb)
                        .build();
                cluster.execute(store);
                store = new StoreValue.Builder()
                        .withBucketType(Test.bucketType)
                        .withBucket(bucket)
                        .withContent('value')
                        .withCallback(cb)
                        .build();
                cluster.execute(store);
            }
        });
    });
    
    after(function(done) {
        var num = 0;
        var type = 'default';
        var nukeBucket = function() {
            num++;
            if (num === 6) {
                if(type === 'default') {
                    type = Test.bucketType;
                    num = 1;
                    Test.cleanBucket(cluster, type, Test.bucketName + '_lb' + (num - 1), nukeBucket);
                } else {
                    cluster.stop(function (err, rslt) {
                        assert(!err, err);
                        done();
                    });
                }
            } else {
                Test.cleanBucket(cluster, type, Test.bucketName + '_lb' + (num - 1), nukeBucket);
            }
        };
        nukeBucket();
    });
    
    it('should list buckets in the default type', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            if (!resp.done) {
                assert(resp.buckets.length);
            } else {
                done();
            }
        };
        var list = new ListBuckets.Builder()
                .withAllowListing()
                .withCallback(callback)
                .build();
        cluster.execute(list);
    });
        
    it('should list buckets in a non-default type', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            if (!resp.done) {
                assert(resp.buckets.length);
            } else {
                done();
            }
        };
        var list = new ListBuckets.Builder()
                .withAllowListing()
                .withCallback(callback)
                .withBucketType(Test.bucketType)
                .build();
        cluster.execute(list);
    });
});
