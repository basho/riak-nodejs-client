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
var FetchProps = require('../../../lib/commands/kv/fetchbucketprops');
var StoreProps = require('../../../lib/commands/kv/storebucketprops');
var ResetProps = require('../../../lib/commands/kv/resetbucketprops');

var bucketName = Test.bucketName + '_sp';

describe('test-integration-kv-resetbucketprops', function() {
    var cluster;
    before(function(done) {
        cluster = Test.buildCluster(function (err, rslt) {
            assert(!err, err);
            done();
        });
    });
    
    after(function(done) {
        cluster.stop(function (err, rslt) {
            assert(!err, err);
            done();
        });
    });
    
    describe('reset-bucket-props', function() {
        it('store-then-reset-in-default-type', function(done) {
            var cb3 = function(err, resp) {
                assert(!err, err);
                assert.strictEqual(resp.allowMult, false);
                done();
            };
            var cb2 = function(err, resp) {
                assert(!err, err);
                var fetch = new FetchProps.Builder()
                        .withBucket(bucketName)
                        .withCallback(cb3)
                        .build();
                cluster.execute(fetch);
            };
            var cb1 = function(err, resp) {
                assert(!err, err);
                assert.strictEqual(resp.allowMult, true);
                var reset = new ResetProps.Builder()
                        .withBucket(bucketName)
                        .withCallback(cb2)
                        .build();
                cluster.execute(reset);
            };
            var cb0 = function(err, resp) {
                assert(!err, err);
                var fetch = new FetchProps.Builder()
                        .withBucket(bucketName)
                        .withCallback(cb1)
                        .build();
                cluster.execute(fetch);
            };
            var store = new StoreProps.Builder()
                    .withBucket(bucketName)
                    .withAllowMult(true)
                    .withCallback(cb0)
                    .build();
            cluster.execute(store);
        });
        it('store-then-reset-in-non-default-type', function(done) {
            var cb3 = function(err, resp) {
                assert(!err, err);
                assert.strictEqual(resp.allowMult, true);
                done();
            };
            var cb2 = function(err, resp) {
                assert(!err, err);
                var fetch = new FetchProps.Builder()
                        .withBucketType(Test.bucketType)
                        .withBucket(bucketName)
                        .withCallback(cb3)
                        .build();
                cluster.execute(fetch);
            };
            var cb1 = function(err, resp) {
                assert(!err, err);
                assert.strictEqual(resp.allowMult, false);
                var reset = new ResetProps.Builder()
                        .withBucketType(Test.bucketType)
                        .withBucket(bucketName)
                        .withCallback(cb2)
                        .build();
                cluster.execute(reset);
            };
            var cb0 = function(err, resp) {
                assert(!err, err);
                var fetch = new FetchProps.Builder()
                        .withBucketType(Test.bucketType)
                        .withBucket(bucketName)
                        .withCallback(cb1)
                        .build();
                cluster.execute(fetch);
            };
            var store = new StoreProps.Builder()
                    .withBucketType(Test.bucketType)
                    .withBucket(bucketName)
                    .withAllowMult(false)
                    .withCallback(cb0)
                    .build();
            cluster.execute(store);
        });
    });
});
