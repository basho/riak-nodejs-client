'use strict';

var FetchSchema = require('../../../lib/commands/yokozuna/fetchschema');
var RpbYokozunaSchemaGetResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbYokozunaSchemaGetResp');
var RpbYokozunaSchema = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbYokozunaSchema');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');
var assert = require('assert');

describe('FetchSchema', function() {
    describe('Build', function() {
        it('should build a RpbYokozunaSchemaGetReq correctly', function(done) {
            
            var fetch = new FetchSchema.Builder()
                    .withSchemaName('schema_name')
                    .withCallback(function(){})
                    .build();
            
            var protobuf = fetch.constructPbRequest();
            
            assert.equal(protobuf.name.toString('utf8'), 'schema_name');
            done();
        });
        
        it('should take a RpbYokozunaSchemaGetResp and call the users callback with the response', function(done) {
            
            var resp = new RpbYokozunaSchemaGetResp();
            var schema = new RpbYokozunaSchema();
            schema.name = new Buffer('mySchemaName');
            schema.content = new Buffer('some XML');
            resp.schema = schema;
            
            var callback = function(err, response) {
                
                assert.equal(response.name, 'mySchemaName');
                assert.equal(response.content, 'some XML');
                done();
            };
            
            var fetch = new FetchSchema.Builder()
                    .withSchemaName('schema_name')
                    .withCallback(callback)
                    .build();
            
            fetch.onSuccess(resp);
            
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
           
           var fetch = new FetchSchema.Builder()
                    .withSchemaName('schema_name')
                    .withCallback(callback)
                    .build();
            
            fetch.onRiakError(rpbErrorResp);
           
           
       });
    });
});
