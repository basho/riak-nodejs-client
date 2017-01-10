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
var StoreIndex = require('../../../lib/commands/yokozuna/storeindex');
var FetchIndex = require('../../../lib/commands/yokozuna/fetchindex');
var DeleteIndex = require('../../../lib/commands/yokozuna/deleteindex');
var rs = require('randomstring');

describe('yokozuna-store-and-fetch', function() {
    var cluster;
    var fetch_attempts = 10;
    var fetch_timeout = 1000;
    var total_timeout = (fetch_attempts * fetch_timeout) + 1000;
    var tmp = 'idx_' + rs.generate(8);

    this.timeout(total_timeout);

    before(function(done) {
        cluster = Test.buildCluster(function (err, rslt) {
            assert(!err, err);
            var callback = function(err, resp) {
                assert(!err, err);
                assert(resp);
                done();
            };
            var store = new StoreIndex.Builder()
                    .withIndexName(tmp)
                    .withCallback(callback)
                    .build();
            cluster.execute(store);
        });
    });

    after(function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp);
            cluster.stop(function (err, rslt) {
                assert(!err, err);
                done();
            });
        };
        var del = new DeleteIndex.Builder()
				.withIndexName(tmp)
				.withCallback(callback)
				.build();
        cluster.execute(del);
    });

    it('fetches', function(done) {
        var count = 0;
        var callback = function(err, resp) {
            count++;
            if (err && err === 'notfound') {
                if (count < fetch_attempts) {
                    setTimeout(fetchme, fetch_timeout);
                } else {
                    assert(!err, err);
                }
            } else {
                assert(resp);
                done();
            }
        };
        var fetchme = function() {
            var fetch = new FetchIndex.Builder()
				.withIndexName(tmp)
				.withCallback(callback)
				.build();
            cluster.execute(fetch);
        };
        fetchme();
    });
});
