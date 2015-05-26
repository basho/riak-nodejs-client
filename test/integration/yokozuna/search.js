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
var Search = require('../../../lib/commands/yokozuna/search');
var StoreProps = require('../../../lib/commands/kv/storebucketprops');
var StoreValue = require('../../../lib/commands/kv/storevalue');

var RiakNode = require('../../../lib/core/riaknode');
var RiakCluster = require('../../../lib/core/riakcluster');

var assert = require('assert');
var logger = require('winston');

describe('Search - Integration', function() {
   
    var cluster;
    this.timeout(30000);
    
    before(function(done) {
        var nodes = RiakNode.buildNodes(Test.nodeAddresses);
        cluster = new RiakCluster({ nodes: nodes});
        cluster.start();
        
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp);
        };
    
        var store = new StoreIndex.Builder()
				.withIndexName('myIndex')
				.withCallback(callback)
				.build();

        cluster.execute(store);

        var prepSearch = function() {
          
            var stuffToStore = [

                "{ \"content_s\":\"Alice was beginning to get very tired of sitting by her sister on the " +
                                    "bank, and of having nothing to do: once or twice she had peeped into the " +
                                    "book her sister was reading, but it had no pictures or conversations in " +
                                    "it, 'and what is the use of a book,' thought Alice 'without pictures or " +
                                    "conversation?'\"}",
                "{ \"content_s\":\"So she was considering in her own mind (as well as she could, for the " +
                                    "hot day made her feel very sleepy and stupid), whether the pleasure " +
                                    "of making a daisy-chain would be worth the trouble of getting up and " +
                                    "picking the daisies, when suddenly a White Rabbit with pink eyes ran " +
                                    "close by her.\", \"multi_ss\":[\"this\",\"that\"]}",

                "{ \"content_s\":\"The rabbit-hole went straight on like a tunnel for some way, and then " +
                                    "dipped suddenly down, so suddenly that Alice had not a moment to think " +
                                    "about stopping herself before she found herself falling down a very deep " +
                                    "well.\"}"


            ];
        
            var count = 0;
            var storeCb = function(err, resp) {
                if (count < 3) {
                    var store = new StoreValue.Builder()
                        .withBucket(Test.bucketName + '_search')
                        .withKey('p' + count)
                        .withContent(stuffToStore[count])
                        .withCallback(storeCb)	
                        .build();

                    count++;
                    cluster.execute(store);
                } else {
                    done();
                }
            };
            
            storeCb();

        };

        var setOnBucket = function () {
            
            var store = new StoreProps.Builder()
				.withBucket(Test.bucketName + '_search')
				.withSearchIndex('myIndex')
				.withCallback(function(err, resp) { assert(!err, err); prepSearch(); })
				.build();

            cluster.execute(store);
            
        };

        var count = 0;
        var fmCallback = function(err, resp) {
            count++;
            if (err) {
                if (err === 'notfound') {
                    if (count < 10) {
                        var sleepMs = 2000 * count;
                        logger.debug("[test/integration/yokozuna/search] sleeping for '%d' ms", sleepMs);
                        setTimeout(fetchme, sleepMs);
                    } else {
                        assert(!err, err);
                    }
                } else {
                    assert(!err, err);
                }
            } else {
                assert(resp);
                setOnBucket();
            }
        };

        var fetchme = function () {
            var fetch = new FetchIndex.Builder()
				.withIndexName('myIndex')
				.withCallback(fmCallback)
				.build();

            cluster.execute(fetch);
        };
        
        fetchme();
    
    });
    
    after(function(done) {
        
        var bpCallback = function(err, resp) {
            assert(!err, err);
            Test.cleanBucket(cluster, 'default', Test.bucketName + '_search', function() {
                cluster.on('stateChange', function(state) { if (state === RiakCluster.State.SHUTDOWN) { done();} });
                cluster.stop();
            });
            
        };
        
        var store = new StoreProps.Builder()
				.withBucket(Test.bucketName + '_search')
				.withSearchIndex('_dont_index_')
				.withCallback(bpCallback)
				.build();

        cluster.execute(store);
        
    });
   
    it('Should perform a search', function(done) {
       
        var count = 0;
        var callback = function(err, resp) {
            if (err || resp.numFound === 0) {
                count++;
                if (count < 10) {
                    setTimeout(searchme, 2000 * count);
                } else {
                    assert(!err, err);
                    assert.fail('Search failed: ' + resp);
                }
            } else {
               done();
            }
        };
        
        var searchme = function() {
            var search = new Search.Builder()
                .withIndexName('myIndex')
                .withQuery('multi_ss:t*')
                .withCallback(callback)
                .build();
            
            cluster.execute(search);
        };
        
        searchme();
        
    });

});
