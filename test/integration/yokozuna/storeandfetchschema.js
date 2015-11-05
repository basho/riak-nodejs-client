'use strict';

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
