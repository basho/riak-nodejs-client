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
var UpdateGSet = require('../../../lib/commands/crdt/updategset');
var FetchSet = require('../../../lib/commands/crdt/fetchset');
var RiakCluster = require('../../../lib/core/riakcluster');

var gset_supported = true;

describe('Update and Fetch GSet - Integration', function() {
    var context;
    var cluster;
    before(function(done) {
        var suite = this;
        cluster = Test.buildCluster(function (err, rslt) {
            assert(!err, err);
            var callback = function(err, resp) {
                if (err) {
                    gset_supported = false;
                    suite.skip();
                } else {
                    assert.equal(resp.values.length, 2);
                    assert.equal(resp.context, null);
                    context = resp.context;
                }
                done();
            };
            var update = new UpdateGSet.Builder()
                    .withBucketType(Test.gsetBucketType)
                    .withBucket(Test.bucketName)
                    .withKey('gset_1')
                    .withAdditions(['this', 'that'])
                    .withCallback(callback)
                    .build();
            cluster.execute(update);
        });
    });

    after(function(done) {
        if (gset_supported) {
            Test.cleanBucket(cluster, Test.gsetBucketType, Test.bucketName, function() {
                cluster.stop(function (err, state) {
                    done();
                });
            });
        } else {
            done();
        }
    });

    it('Should fetch a gset', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.values.length, 2);
            assert.equal(resp.context, null);
            done();
        };
        var fetch = new FetchSet.Builder()
                .withBucketType(Test.gsetBucketType)
                .withBucket(Test.bucketName)
                .withKey('gset_1')
                .withCallback(callback)
                .build();
        cluster.execute(fetch);
    });

    it('Should report isNotFound if a gset does not exist', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp.notFound);
            assert(resp.isNotFound);
            done();
        };
        var fetch = new FetchSet.Builder()
                .withBucketType(Test.gsetBucketType)
                .withBucket(Test.bucketName)
                .withKey('gset_notFound')
                .withCallback(callback)
                .build();
        cluster.execute(fetch);
    });

    it('Should add to a gset', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert.equal(resp.values.length, 3);
            done();
        };

        var update = new UpdateGSet.Builder()
                .withBucketType(Test.gsetBucketType)
                .withBucket(Test.bucketName)
                .withKey('gset_1')
                .withAdditions(['those'])
                .withContext(context)
                .withCallback(callback)
                .build();

        cluster.execute(update);
    });
});
