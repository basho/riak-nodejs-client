'use strict';

var StoreIndex = require('../../../lib/commands/yokozuna/storeindex');
var RpbYokozunaIndex = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbYokozunaIndex');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');

var assert = require('assert');

describe('StoreIndex', function() {
    describe('Build', function() {
        it('should build a RpbYokozunaIndexPutReq correctly', function(done) {
            
            var store = new StoreIndex.Builder()
                    .withIndexName('indexName')
                    .withSchemaName('schemaName')
                    .withNVal(5)
                    .withTimeout(60000)
                    .withCallback(function(){})
                    .build();
            
            var protobuf = store.constructPbRequest();
            assert.equal(protobuf.getTimeout(), 60000);

            var index = protobuf.index;
            assert.equal(index.getName().toString('utf8'), 'indexName');
            assert.equal(index.getSchema().toString('utf8'), 'schemaName');
            assert.equal(index.getNVal(), 5);
            done();
        });

        it('should only require name and callback', function(done) {

            assert.throws(function () {
                new StoreIndex.Builder().build();
            });

            assert.throws(function () {
                new StoreIndex.Builder()
                    .withIndexName('indexName')
                    .build();
            });

            assert.throws(function () {
                new StoreIndex.Builder()
                    .withCallback(function () {})
                    .build();
            });

            assert.doesNotThrow(function () {
                new StoreIndex.Builder()
                    .withIndexName('indexName')
                    .withCallback(function () {})
                    .build();
            });

            done();
        });
    });
    
    it('should take a RpbPutResp and call the users callback with the response', function(done) {
            
        var callback = function(err, response) {
            assert.equal(response, true);
            done();
        };
        
        var store = new StoreIndex.Builder()
                .withIndexName('indexName')
                .withCallback(callback)
                .build();
        
        // the RpbPutResp is a null message (no body) from riak
        store.onSuccess(null);
        
    });
        
    it ('should take a RpbErrorResp and call the users callback with the error message', function(done) {
        var rpbErrorResp = new RpbErrorResp();
        rpbErrorResp.setErrmsg(new Buffer('this is an error'));
        
        var callback = function(err, response) {
            if (err) {
                assert.equal(err,'this is an error');
                done();
            }
        };
        
        var store = new StoreIndex.Builder()
                .withIndexName('indexName')
                .withCallback(callback)
                .build();
        
        store.onRiakError(rpbErrorResp);
        
    });
    
});
