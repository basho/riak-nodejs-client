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
var UpdateCounter = require('../../../lib/commands/crdt/updatecounter');
var FetchCounter = require('../../../lib/commands/crdt/fetchcounter');

describe('integration-crdt-counter', function() {
    var cluster;
    before(function(done) {
        cluster = Test.buildCluster(function (err, rslt) {
            assert(!err, err);
            var callback = function(err, resp) {
                assert(!err, err);
                done();
            };
            var update = new UpdateCounter.Builder()
                    .withBucketType(Test.counterBucketType)
                    .withBucket(Test.bucketName)
                    .withKey('counter_1')
                    .withIncrement(10)
                    .withCallback(callback)
                    .build();
            cluster.execute(update);
        });
    });
    
    after(function(done) {
        Test.cleanBucket(cluster, Test.counterBucketType, Test.bucketName, function() {
            cluster.stop(function (err, rslt) {
                assert(!err, err);
                done();
            });
        });
    });
    
    it('fetches-counter', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.counterValue, 10);
            done();
        };
        var fetch = new FetchCounter.Builder()
            .withKey('counter_1')
            .withBucketType(Test.counterBucketType)
            .withBucket(Test.bucketName)
            .withCallback(callback)
            .build();
        cluster.execute(fetch);
    });
    
    it('returns-isNotFound', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp.notFound);
            assert(resp.isNotFound);
            done();
        };
        var fetch = new FetchCounter.Builder()
            .withKey('counter_notFound')
            .withBucketType(Test.counterBucketType)
            .withBucket(Test.bucketName)
            .withCallback(callback)
            .build();
        cluster.execute(fetch);
    });
    
    it('updates-counter', function(done) {
       var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.counterValue, 20);
            done();
        };
        var update = new UpdateCounter.Builder()
                .withKey('counter_1')
                .withBucketType(Test.counterBucketType)
                .withBucket(Test.bucketName)
                .withIncrement(10)
                .withCallback(callback)
                .build();
        
        cluster.execute(update);
    });
    
    it('generates-key', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.counterValue, 10);
            assert(resp.generatedKey);
            done();
        };
        var update = new UpdateCounter.Builder()
                .withBucketType(Test.counterBucketType)
                .withBucket(Test.bucketName)
                .withIncrement(10)
                .withCallback(callback)
                .build();
        cluster.execute(update);
    });
});
