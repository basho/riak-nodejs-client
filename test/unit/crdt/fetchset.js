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

var FetchSet = require('../../../lib/commands/crdt/fetchset');
var Rpb = require('../../../lib/protobuf/riakprotobuf');
var DtFetchReq = Rpb.getProtoFor('DtFetchReq');
var DtFetchResp = Rpb.getProtoFor('DtFetchResp');
var DtValue = Rpb.getProtoFor('DtValue');
var RpbErrorResp = Rpb.getProtoFor('RpbErrorResp');

var ByteBuffer = require('bytebuffer');
var assert = require('assert');

describe('FetchSet', function() {
    describe('Build', function() {
        var builder = new FetchSet.Builder().
        withBucketType('sets_type').
        withBucket('set_bucket').
        withKey('cool_set');

        it('builds a DtFetchSet correctly', function(done){
            var fetchSet = builder.
                    withCallback(function(){}).
                    withR(1).
                    withPr(2).
                    withNotFoundOk(true).
                    withUseBasicQuorum(true).
                    withTimeout(12345).
                    build();

            var protobuf = fetchSet.constructPbRequest();

            assert.equal(protobuf.getType().toString('utf8'),
                         'sets_type');
            assert.equal(protobuf.getBucket().toString('utf8'),
                         'set_bucket');
            assert.equal(protobuf.getKey().toString('utf8'),
                         'cool_set');
            assert.equal(protobuf.getR(), 1);
            assert.equal(protobuf.getPr(), 2);
            assert.equal(protobuf.getNotfoundOk(), true);
            assert.equal(protobuf.getBasicQuorum(), true);
            assert.equal(protobuf.getTimeout(), 12345);
            done();
        });

        it('calls back with successful results', function(done){
            var resp = new DtFetchResp();
            resp.type = 2;
            resp.context = new Buffer("asdf");

            var value = new DtValue();
            resp.setValue(value);

            value.set_value = [new Buffer("zedo"),
                               new Buffer("piper"),
                               new Buffer("little one")];

            var includesBuffer = function(haystack, needle) {
                var needleBuf = new Buffer(needle);
                var len = haystack.length;
                for (var i = 0; i < len; i++) {
                    if (haystack[i].equals(needleBuf)) return true;
                }
                
                return false;
            };

            var includes = function(haystack, needle) {
                var len = haystack.length;
                for (var i = 0; i < len; i++) {
                    if (haystack[i] === needle) return true;
                }

                return false;
            };
            
            var callback = function(err, response) {
                assert.equal(response.context.toString("utf8"), "asdf");
                assert.equal(response.dataType, 2);

                assert(includesBuffer(response.valueBuffers, "zedo"));
                assert(includes(response.value, "zedo"));

                assert(includesBuffer(response.valueBuffers, "piper"));
                assert(includes(response.value, "piper"));
                
                assert(includesBuffer(response.valueBuffers, "little one"));
                assert(includes(response.value, "little one"));
                
                done();
            };

            var fetch = builder.
                    withCallback(callback).
                    build();

            fetch.onSuccess(resp);
        });
        
        it('calls back with an error message', function(done) {
            var errorMessage = "couldn't crdt :(";
            var errorResp = new RpbErrorResp();
            errorResp.setErrmsg(new Buffer(errorMessage));

            var callback = function(err, response) {
                assert(err);
                assert.equal(err, errorMessage);

                done();
            };

            var fetch = builder.
                    withCallback(callback).
                    build();

            fetch.onRiakError(errorResp);
        });
    });
});
