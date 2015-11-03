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
