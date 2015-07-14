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
var StoreIndex = require('../../../lib/commands/yokozuna/storeindex');
var FetchIndex = require('../../../lib/commands/yokozuna/fetchindex');
var DeleteIndex = require('../../../lib/commands/yokozuna/deleteindex');

var RiakNode = require('../../../lib/core/riaknode');
var RiakCluster = require('../../../lib/core/riakcluster');
var assert = require('assert');

describe('Update and Fetch Yokozuna index - Integration', function() {
    var cluster;
    this.timeout(30000);
    
    before(function(done) {
        var nodes = RiakNode.buildNodes(Test.nodeAddresses);
        cluster = new RiakCluster({ nodes: nodes});
        cluster.start();
        
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp);
            done();
        };
    
        var store = new StoreIndex.Builder()
				.withIndexName('myIndex')
				.withCallback(callback)
				.build();
        cluster.execute(store);
    });
    
    after(function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp);
            cluster.on('stateChange', function(state) { if (state === RiakCluster.State.SHUTDOWN) { done();} });
            cluster.stop();
        };
        var del = new DeleteIndex.Builder()
				.withIndexName('myIndex')
				.withCallback(callback)
				.build();
        cluster.execute(del);
    });
    
    it('Should fetch an index', function(done) {
        var count = 0;
        var callback = function(err, resp) {
            count++;
            if(err && err === 'notfound') {
                if (count < 6) {
                    setTimeout(fetchme, 2000 * count);
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
				.withIndexName('myIndex')
				.withCallback(callback)
				.build();
            cluster.execute(fetch);
        };
        fetchme();
    });
});
