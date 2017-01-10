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

var table = 'test-table';
var key = [ 'foo', 'bar', 'baz' ];

describe('Get', function() {
    describe('Build', function() {
        it('should build a TsGetReq correctly', function(done) {
            var cmd = new TS.Get.Builder()
               .withTable(table)
               .withKey(key)
               .withCallback(function(){})
               .build();
            var protobuf = cmd.constructPbRequest();
            assert.strictEqual(protobuf.getTable().toString('utf8'), table);

            var tscells = protobuf.getKey();
            for (var i = 0; i < key.length; i++) {
                var tsc = tscells[i];
                assert.strictEqual(tsc.varchar_value.toString('utf8'), key[i]);
            }

            done();
        });
        
        it('should take a TsGetResp and call the users callback with the response', function(done) {
            var cb = function(err, response) {
                assert(!err, err);
                d.validateResponse(response, d.tsGetResp);
                done();
            };
            var cmd = new TS.Get.Builder()
               .withTable(table)
               .withKey(key)
               .withCallback(cb)
               .build();
            cmd.onSuccess(d.tsGetResp);
        });
        
        it ('should take a RpbErrorResp and call the users callback with the error message', function(done) {
           var rpbErrorResp = new RpbErrorResp();
           rpbErrorResp.setErrmsg(new Buffer('this is an error'));
           rpbErrorResp.setErrcode(11);
           var cb = function(err, response, errdata) {
                assert(err, !err);
                assert(!response);
                assert(errdata, !errdata);
                assert.strictEqual(errdata.msg, 'this is an error');
                assert.strictEqual(errdata.code, 11);
                done();
            };
            var cmd = new TS.Get.Builder()
               .withTable(table)
               .withKey(key)
               .withCallback(cb)
               .build();
            cmd.onRiakError(rpbErrorResp);
        });
    });
});
