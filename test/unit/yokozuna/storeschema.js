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
