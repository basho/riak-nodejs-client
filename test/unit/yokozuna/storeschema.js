'use strict';

var StoreSchema = require('../../../lib/commands/yokozuna/storeschema');
var RpbYokozunaIndex = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbYokozunaIndex');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');

var assert = require('assert');

describe('StoreSchema', function() {
    describe('Build', function() {
        it('should build a RpbYokozunaSchemaPutReq correctly', function(done) {
            
            var store = new StoreSchema.Builder()
                    .withSchemaName('schemaName')
                    .withSchema('some XML')
                    .withCallback(function(){})
                    .build();
            
            var protobuf = store.constructPbRequest();
            var schema = protobuf.schema;
            assert.equal(schema.getName().toString('utf8'), 'schemaName');
            assert.equal(schema.getContent().toString('utf8'), 'some XML');
            done();
        });
        
        it('should take a RpbPutResp and call the users callback with the response', function(done) {
           
            
            var callback = function(err, response) {
              
                assert.equal(response, true);
                done();
            };
            
            var store = new StoreSchema.Builder()
                    .withSchemaName('schemaName')
                    .withSchema('some XML')
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
           
           var store = new StoreSchema.Builder()
                    .withSchemaName('schemaName')
                    .withSchema('some XML')
                    .withCallback(callback)
                    .build();
            
            store.onRiakError(rpbErrorResp);
           
       });

    });
});
