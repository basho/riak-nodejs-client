/**
 *
 * Copyright 2014-present Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

var FetchIndex = require('../../../lib/commands/yokozuna/fetchindex');
var RpbYokozunaIndexGetResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbYokozunaIndexGetResp');
var RpbYokozunaIndex = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbYokozunaIndex');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');

var assert = require('assert');

describe('FetchIndex', function() {
    describe('Build', function() {
        it('should build a RpbYokozunaIndexGetReq correctly', function(done) {
            
            var fetch = new FetchIndex.Builder()
                    .withIndexName('indexName')
                    .withCallback(function(){})
                    .build();
            
            var protobuf = fetch.constructPbRequest();
            
            assert.equal(protobuf.getName().toString('utf8'), 'indexName');
            done();
            
        });
        
        it('should take a RpbYokozunaIndexGetResp and call the users callback with the response', function(done) {
           
            var resp = new RpbYokozunaIndexGetResp();
            
            for (var i =0; i < 3; i++) {
                var index = new RpbYokozunaIndex();

                index.name = new Buffer('myIndex_' + i);
                index.schema = new Buffer('mySchema_' + i);
                index.n_val = i;

                resp.index.push(index);
            }
            
            var callback = function(err, response) {
              
                assert.equal(response.length, 3);
                assert.equal(response[1].indexName, 'myIndex_1');
                assert.equal(response[1].schemaName, 'mySchema_1');
                assert.equal(response[1].nVal, 1);
                done();
            };
            
            var fetch = new FetchIndex.Builder()
                    .withIndexName('indexName')
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
           
           var fetch = new FetchIndex.Builder()
                    .withIndexName('indexName')
                    .withCallback(callback)
                    .build();
            
            fetch.onRiakError(rpbErrorResp);
           
       });
        
    });
});
