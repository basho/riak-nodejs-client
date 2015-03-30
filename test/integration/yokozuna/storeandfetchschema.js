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
var StoreSchema = require('../../../lib/commands/yokozuna/storeschema');
var FetchSchema = require('../../../lib/commands/yokozuna/fetchschema');
var RiakNode = require('../../../lib/core/riaknode');
var RiakCluster = require('../../../lib/core/riakcluster');
var assert = require('assert');

describe('Update and Fetch Yokozuna schema - Integration', function() {
   
    var cluster;
    this.timeout(10000);
    var defaultSchema;
    
    before(function(done) {
        var nodes = RiakNode.buildNodes(Test.nodeAddresses);
        cluster = new RiakCluster({ nodes: nodes});
        cluster.start();
       
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp.content);
            defaultSchema = resp.content;
            done();
        };
        
        var fetch = new FetchSchema.Builder()
				.withSchemaName('_yz_default')
				.withCallback(callback)
				.build();

        cluster.execute(fetch);

        
    });
    
    after(function(done) {
       cluster.on('stateChange', function(state) { if (state === RiakCluster.State.SHUTDOWN) { done();} });
       cluster.stop(); 
    });
    
    it('Should store a schema', function(done) {
       
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp);
            done();
        };
       
        var store = new StoreSchema.Builder()
					.withSchemaName('mySchema')
					.withSchema(defaultSchema)
					.withCallback(callback)
					.build();

	cluster.execute(store);	
        
    });
    
    it('Should fetch a schema', function(done) {
        var callback = function(err, resp) {
            assert(!err, err);
            assert(resp);
            done();
        };
        
        var fetch = new FetchSchema.Builder()
				.withSchemaName('mySchema')
				.withCallback(callback)
				.build();

        cluster.execute(fetch);
        
    });
    
});