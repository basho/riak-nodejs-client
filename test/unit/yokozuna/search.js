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

var Search = require('../../../lib/commands/yokozuna/search');
var RpbPair = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbPair');
var RpbSearchDoc = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbSearchDoc');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');
var RpbSearchQueryResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbSearchQueryResp');
var assert = require('assert');

describe('Search', function() {
    describe('Build', function() {
        it('should build a RpbSearchQueryReq correctly', function(done) {
        
            var search = new Search.Builder()
                    .withIndexName('indexName')
                    .withQuery('some solr query')
                    .withNumRows(20)
                    .withStart(10)
                    .withSortField('someField')
                    .withFilterQuery('someQuery')
                    .withDefaultField('defaultField')
                    .withDefaultOperation('and')
                    .withReturnFields(['field1', 'field2'])
                    .withPresort('key')
                    .withCallback(function(){})
                    .build();
            
            var protobuf = search.constructPbRequest();
            
            assert.equal(protobuf.index.toString('utf8'), 'indexName');
            assert.equal(protobuf.q.toString('utf8'), 'some solr query');
            assert.equal(protobuf.rows, 20);
            assert.equal(protobuf.start, 10);
            assert.equal(protobuf.sort.toString('utf8'), 'someField');
            assert.equal(protobuf.filter.toString('utf8'), 'someQuery');
            assert.equal(protobuf.df.toString('utf8'), 'defaultField');
            assert.equal(protobuf.op.toString('utf8'), 'and');
            assert.equal(protobuf.fl.length, 2);
            assert.equal(protobuf.fl[0].toString('utf8'), 'field1');
            assert.equal(protobuf.fl[1].toString('utf8'), 'field2');
            assert.equal(protobuf.presort.toString('utf8'), 'key');
            done();
            
        
        });
        
        it('should take a RpbSearchQueryResp and call the users callback with the response', function(done) {
            
            var resp = new RpbSearchQueryResp();
            
            var doc = new RpbSearchDoc();
            // The PB API is broken in that it returns everything as strings. The 
            // search command should convert boolean and numeric values properly
            var pair = new RpbPair();
            pair.key = new Buffer('leader_b');
            pair.value = new Buffer('true');
            doc.fields.push(pair);
            
            pair = new RpbPair();
            pair.key = new Buffer('age_i');
            pair.value = new Buffer('30');
            doc.fields.push(pair);
            
            pair = new RpbPair();
            pair.key = new Buffer('_yz_id');
            pair.value = new Buffer('default_cats_liono_37');
            doc.fields.push(pair);
            
            pair = new RpbPair();
            pair.key = new Buffer('nullValue');
            pair.value = new Buffer('null');
            doc.fields.push(pair);
            
            resp.docs.push(doc);
            resp.max_score = 1.123;
            resp.num_found = 1;
            
            var callback = function(err, response) {
              
                assert.equal(response.numFound, 1);
                assert.equal(response.maxScore, 1.123);
                assert.equal(response.docs.length, 1);
                var doc = response.docs[0];
                assert.equal(doc.leader_b, true); // converted to boolean
                assert.equal(doc.age_i, 30); // converted to number
                assert.equal(doc.nullValue, null); // converted to null
                assert.equal(doc._yz_id, 'default_cats_liono_37'); // left as string
                done();
            };
            
            var search = new Search.Builder()
                    .withIndexName('indexName')
                    .withQuery('some solr query')
                    .withCallback(callback)
                    .build();
            
            search.onSuccess(resp);
            
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
           
           var search = new Search.Builder()
                    .withIndexName('indexName')
                    .withQuery('some solr query')
                    .withCallback(callback)
                    .build();
            
           search.onRiakError(rpbErrorResp);
           
           
       });
        
    });
});
