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

var rpb = require('../../../lib/protobuf/riakprotobuf');
var ListKeys = require('../../../lib/commands/ts/listkeys');
var TsListKeysResp = rpb.getProtoFor('TsListKeysResp');
var TsCell = rpb.getProtoFor('TsCell');
var TsRow = rpb.getProtoFor('TsRow');
var RpbErrorResp = rpb.getProtoFor('RpbErrorResp');

var assert = require('assert');

function doCallListKeysOnSuccess(listKeysCmd) {
    var tsc = new TsCell();
    tsc.setVarcharValue(new Buffer('foo', 'utf8'));
    var tsr = new TsRow();
    tsr.cells.push(tsc);

    for (var i = 0; i < 20; i++) {
        var listKeysResp = new TsListKeysResp();    
        for (var j = 0; j < 5; j++) {
            listKeysResp.keys.push(tsr);
        }
        if (i === 19) {
            listKeysResp.done = true;
        }
        listKeysCmd.onSuccess(listKeysResp);
    }
}

describe('ListKeys', function() {
    describe('Build', function() {
        it('should build a RpbTsListKeysReq correctly', function(done) {
            var listKeys = new ListKeys.Builder()
                .withTable('table')
                .withTimeout(1234)
                .withCallback(function(){})
                .build();
            var protobuf = listKeys.constructPbRequest();
            assert.equal(protobuf.getTable().toString('utf8'), 'table');
            assert.equal(protobuf.getTimeout(), 1234);
            done();
        });

        it('should take multiple TsListKeysResp and call the users callback with the response', function(done) {
            var callback = function(err, resp){
                assert.equal(resp.keys.length, 100);
                assert.equal(resp.done, true);
                done();
            };
            var listKeys = new ListKeys.Builder()
                    .withTable('table')
                    .withStreaming(false)
                    .withCallback(callback)
                    .build();
            doCallListKeysOnSuccess(listKeys);
        });
        
        it('should take multiple TsListKeysResp and stream the response', function(done) {
            var count = 0;
            var timesCalled = 0;
            var callback = function (err, resp) {
                timesCalled++;
                count += resp.keys.length;
                if (resp.done) {
                    assert.equal(timesCalled, 20);
                    assert.equal(count, 100);
                    done();
                }
            };
            var listKeys = new ListKeys.Builder()
                    .withTable('table')
                    .withCallback(callback)
                    .build();
            doCallListKeysOnSuccess(listKeys);
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
           var listKeys = new ListKeys.Builder()
                    .withTable('table')
                    .withStreaming(false)
                    .withCallback(callback)
                    .build();
            listKeys.onRiakError(rpbErrorResp);
       });
    });
});
