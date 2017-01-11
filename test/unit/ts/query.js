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

var d = require('./data');
var TS = require('../../../lib/commands/ts');

var rpb = require('../../../lib/protobuf/riakprotobuf');
var RpbErrorResp = rpb.getProtoFor('RpbErrorResp');

var assert = require('assert');
if (!assert.deepStrictEqual) {
    assert.deepStrictEqual = assert.deepEqual;
}

var logger = require('winston');
var Long = require('long');

var queryText = 'select * from foo where baz = "bat"';

describe('Query', function() {
    describe('Build', function() {
        it('should build a TsQueryReq correctly', function(done) {
            var queryCommand = new TS.Query.Builder()
               .withQuery(queryText)
               .withCallback(function(){})
               .build();
            var protobuf = queryCommand.constructPbRequest();

            var tsi = protobuf.getQuery();
            assert(tsi, 'expected Tsinterpolation');

            var base = tsi.getBase().toString('utf8');
            assert.strictEqual(base, queryText);

            var i = tsi.getInterpolations();
            assert(Array.isArray(i));
            assert.strictEqual(i.length, 0);

            done();
        });
        
        it('should take a TsQueryResp and call the users callback with the response', function(done) {
            var cb = function(err, response) {
                assert(!err, err);
                d.validateResponse(response, d.tsQueryResp);
                done();
            };
            var queryCommand = new TS.Query.Builder()
               .withQuery(queryText)
               .withCallback(cb)
               .build();
            queryCommand.onSuccess(d.tsQueryResp);
        });
        
        it ('should take a RpbErrorResp and call the users callback with the error message', function(done) {
           var rpbErrorResp = new RpbErrorResp();
           rpbErrorResp.setErrmsg(new Buffer('this is an error'));
           
           var cb = function(err, response) {
                assert(err, !err);
                assert.strictEqual(err, 'this is an error');
                done();
            };
           
            var queryCommand = new TS.Query.Builder()
               .withQuery(queryText)
               .withCallback(cb)
               .build();
       
            queryCommand.onRiakError(rpbErrorResp);
        });
    });
});
