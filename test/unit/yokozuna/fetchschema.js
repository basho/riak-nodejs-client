'use strict';

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
