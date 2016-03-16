'use strict';

var assert = require('assert');

var Test = require('../testparams');
var StoreSchema = require('../../../lib/commands/yokozuna/storeschema');
var FetchSchema = require('../../../lib/commands/yokozuna/fetchschema');

describe('Update and Fetch Yokozuna schema - Integration', function() {
    var defaultSchema;
    var cluster;
    before(function(done) {
        cluster = Test.buildCluster(function (err, rslt) {
            assert(!err, err);
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
    });
    
    after(function(done) {
        cluster.stop(function (err, rslt) {
            assert(!err, err);
            done();
        });
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
