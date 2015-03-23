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

var DeleteIndex = require('../../../lib/commands/yokozuna/deleteindex');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');

var assert = require('assert');


describe('DeleteIndex', function() {
    describe('Build', function() {
        it('should build a RpbYokozunaIndexDeleteReq correctly', function(done) {
            
            var del = new DeleteIndex.Builder()
                    .withIndexName('indexName')
                    .withCallback(function(){})
                    .build();
            
            var protobuf = del.constructPbRequest();
            
            assert.equal(protobuf.getName().toString('utf8'), 'indexName');
            done();
            
        });
        
        it('should take a RpbDelResp and call the users callback with the response', function(done) {
           
            
            var callback = function(err, response) {
              
                assert.equal(response, true);
                done();
            };
            
            var del = new DeleteIndex.Builder()
                    .withIndexName('indexName')
                    .withCallback(callback)
                    .build();
            
            // a RpbDelResp is a null message (no body) from riak
            del.onSuccess(null);
            
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
           
           var del = new DeleteIndex.Builder()
                    .withIndexName('indexName')
                    .withCallback(callback)
                    .build();
            
            del.onRiakError(rpbErrorResp);
           
       });

        

    });
});
