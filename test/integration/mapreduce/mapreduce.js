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
var StoreProps = require('../../../lib/commands/kv/storebucketprops');
var MapReduce = require('../../../lib/commands/mapreduce/mapreduce');
var StoreValue = require('../../../lib/commands/kv/storevalue');

var RiakNode = require('../../../lib/core/riaknode');
var RiakCluster = require('../../../lib/core/riakcluster');
var assert = require('assert');
var logger = require('winston');

describe('MapReduce - Integration', function() {
   
	var mrBucketName = Test.bucketName + '_mr';

    var cluster;
    this.timeout(30000);
    
    before(function(done) {
        var nodes = RiakNode.buildNodes(Test.nodeAddresses);
        cluster = new RiakCluster({ nodes: nodes});
        cluster.start();
        
        var stuffToStore = [
            "Alice was beginning to get very tired of sitting by her sister on the " +
                                "bank, and of having nothing to do: once or twice she had peeped into the " +
                                "book her sister was reading, but it had no pictures or conversations in " +
                                "it, 'and what is the use of a book,' thought Alice 'without pictures or " +
                                "conversation?'",
            "So she was considering in her own mind (as well as she could, for the " +
                                "hot day made her feel very sleepy and stupid), whether the pleasure " +
                                "of making a daisy-chain would be worth the trouble of getting up and " +
                                "picking the daisies, when suddenly a White Rabbit with pink eyes ran " +
                                "close by her.",
            "The rabbit-hole went straight on like a tunnel for some way, and then " +
                                "dipped suddenly down, so suddenly that Alice had not a moment to think " +
                                "about stopping herself before she found herself falling down a very deep " +
                                "well."
        ];
        
        var count = 0;
        var storeCb = function(err, resp) {
            if (count < 3) {
                var store = new StoreValue.Builder()
                    .withBucket(mrBucketName)
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
    });
    
    after(function(done) {
       Test.cleanBucket(cluster, 'default', mrBucketName, function() {
            cluster.on('stateChange', function(state) { if (state === RiakCluster.State.SHUTDOWN) { done();} });
            cluster.stop();
        }); 
    });
    
    it('Should Map and Reduce', function(done) {
        var callback = function(err, resp) {
			logger.debug("[TestMapReduce] resp: %s", JSON.stringify(resp));
            assert(!err, err);
            if (resp.done) {
                done();
            } else {
                assert(resp.response.length > 0);
            }
        };
       
        var query = "{\"inputs\":[[\"" + mrBucketName +"\",\"p0\"]," +
            "[\"" + mrBucketName + "\",\"p1\"]," +
            "[\"" + mrBucketName + "\",\"p2\"]]," +
            "\"query\":[{\"map\":{\"language\":\"javascript\",\"source\":\"" +
            "function(v) {var m = v.values[0].data.toLowerCase().match(/\\w*/g); var r = [];" +
            "for(var i in m) {if(m[i] != '') {var o = {};o[m[i]] = 1;r.push(o);}}return r;}" +
            "\"}},{\"reduce\":{\"language\":\"javascript\",\"source\":\"" +
            "function(v) {var r = {};for(var i in v) {for(var w in v[i]) {if(w in r) r[w] += v[i][w];" +
            "else r[w] = v[i][w];}}return [r];}\"}}]}";
		var mr = new MapReduce(query, callback);
		cluster.execute(mr);
    });
});
