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

var UpdateGSet = require('../../../lib/commands/crdt/updategset');
var Rpb = require('../../../lib/protobuf/riakprotobuf');
var DtUpdateReq = Rpb.getProtoFor('DtUpdateReq');
var DtUpdateResp = Rpb.getProtoFor('DtUpdateResp');
var DtValue = Rpb.getProtoFor('DtValue');
var RpbErrorResp = Rpb.getProtoFor('RpbErrorResp');

var ByteBuffer = require('bytebuffer');
var assert = require('assert');
var utils = require('../../utils');

describe('UpdateGSet', function() {
    // ikea rugs
    var hampenBuffer = ByteBuffer.fromUTF8("hampen");
    var snabbfotadBuffer = ByteBuffer.fromUTF8("snabbfotad");

    var someContext = ByteBuffer.fromUTF8("context");

    var builder = new UpdateGSet.Builder().
            withBucketType('gsets').
            withBucket('gset_bucket').
            withKey('cool_gset');

    describe('Build', function() {
        it('builds a DtUpdateSet correctly', function(done){
            var update = builder.
                    withContext(someContext).
                    withAdditions(["gåser", hampenBuffer]).
                    withCallback(function(){}).
                    withW(1).
                    withDw(2).
                    withPw(3).
                    withReturnBody(false).
                    withTimeout(12345).
                    build();

            var protobuf = update.constructPbRequest();

            assert.equal(protobuf.getType().toString('utf8'), 'gsets');
            assert.equal(protobuf.getBucket().toString('utf8'), 'gset_bucket');
            assert.equal(protobuf.getKey().toString('utf8'), 'cool_gset');
            assert.equal(protobuf.getW(), 1);
            assert.equal(protobuf.getDw(), 2);
            assert.equal(protobuf.getPw(), 3);
            assert.equal(protobuf.getReturnBody(), false);
            assert.equal(protobuf.getTimeout(), 12345);
            assert(utils.includesBuffer(protobuf.op.gset_op.adds, "gåser"));
            assert(utils.includesBuffer(protobuf.op.gset_op.adds, "hampen"));
            assert.equal(protobuf.getContext().toString('utf8'), 'context');
            done();
        });
    });

    describe('with body, without key, as buffers', function() {
        it('calls back with successful results', function(done) {
            var resp = new DtUpdateResp();
            resp.setContext(new Buffer("asdf"));
            resp.setGsetValue([hampenBuffer, snabbfotadBuffer]);

            var callback = function(err, response) {
                assert(!err);
                assert(response);

                assert.equal(response.context.toString("utf8"), "asdf");
                assert(utils.includesBuffer(response.values, "hampen"));
                assert(utils.includesBuffer(response.values, "snabbfotad"));

                assert.equal(response.generatedKey, null);
                done();
            };

            var update = builder.
                    withCallback(callback).
                    withAdditions([hampenBuffer]).
                    withSetsAsBuffers(true).
                    build();

            update.onSuccess(resp);
        });
    });

    describe('with body, without key, as strings', function() {
        it('calls back with successful results', function(done) {
            var resp = new DtUpdateResp();
            resp.setContext(new Buffer("asdf"));
            resp.setGsetValue([hampenBuffer, snabbfotadBuffer]);

            var callback = function(err, response) {
                assert(!err);
                assert(response);

                assert.equal(response.context.toString("utf8"), "asdf");
                assert(utils.includes(response.values, "hampen"));
                assert(utils.includes(response.values, "snabbfotad"));

                assert.equal(response.generatedKey, null);
                done();
            };

            var update = builder.
                    withCallback(callback).
                    withAdditions([hampenBuffer]).
                    withSetsAsBuffers(false).
                    build();

            update.onSuccess(resp);
        });
    });

    describe('without body, with key', function() {
        it('calls back with successful results', function(done) {
            var resp = new DtUpdateResp();
            resp.key = new Buffer("ikea_rugs");

            var callback = function(err, response) {
                assert.equal(response.generatedKey, "ikea_rugs");
                done();
            };

            var update = builder.
                    withCallback(callback).
                    withAdditions([hampenBuffer]).
                    build();

            update.onSuccess(resp);
        });
    });

    describe('returning an error', function() {
        it('calls back with an error message', function(done) {
            var errorMessage = "couldn't crdt :(";
            var errorResp = new RpbErrorResp();
            errorResp.setErrmsg(new Buffer(errorMessage));

            var callback = function(err, response) {
                assert(err);
                assert(!response);

                assert.equal(err, errorMessage);

                done();
            };

            var update = builder.
                    withCallback(callback).
                    build();

            update.onRiakError(errorResp);
        });
    });
});
