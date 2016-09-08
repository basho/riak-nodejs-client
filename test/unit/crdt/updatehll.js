'use strict';

var UpdateHll = require('../../../lib/commands/crdt/updatehll');
var Rpb = require('../../../lib/protobuf/riakprotobuf');
var DtUpdateReq = Rpb.getProtoFor('DtUpdateReq');
var DtUpdateResp = Rpb.getProtoFor('DtUpdateResp');
var DtValue = Rpb.getProtoFor('DtValue');
var RpbErrorResp = Rpb.getProtoFor('RpbErrorResp');

var ByteBuffer = require('bytebuffer');
var assert = require('assert');

describe('UpdateHll', function() {

  var someBuffer = ByteBuffer.fromUTF8("Some");
  var jokesBuffer = ByteBuffer.fromUTF8("Jokes");

    var builder = new UpdateHll.Builder().
            withBucketType('hlls').
            withBucket('myBucket').
            withKey('hll_1');

    describe('Build', function() {
        var includesBuffer = function(haystack, needle) {
            var len = haystack.length;
            for (var i = 0; i < len; i++) {
                if (haystack[i].toString('utf8') === needle) {
                    return true;
                }
            }

            return false;
        };

        it('builds a DtUpdateHll correctly', function(done){
            var update = builder.
                    withAdditions(['Jokes', 'Are', 'Better', 'Explained']).
                    withCallback(function(){}).
                    withW(1).
                    withDw(2).
                    withPw(3).
                    withReturnBody(false).
                    withTimeout(12345).
                    build();

            var protobuf = update.constructPbRequest();

            assert.equal(protobuf.getType().toString('utf8'), 'hlls');
            assert.equal(protobuf.getBucket().toString('utf8'), 'myBucket');
            assert.equal(protobuf.getKey().toString('utf8'), 'hll_1');
            assert.equal(protobuf.getW(), 1);
            assert.equal(protobuf.getDw(), 2);
            assert.equal(protobuf.getPw(), 3);

            assert.equal(protobuf.getReturnBody(), false);
            assert.equal(protobuf.getTimeout(), 12345);

            assert(includesBuffer(protobuf.op.hll_op.adds,
                                  "Jokes"));
            assert(includesBuffer(protobuf.op.hll_op.adds,
                                  "Are"));
            assert(includesBuffer(protobuf.op.hll_op.adds,
                                  "Better"));
            assert(includesBuffer(protobuf.op.hll_op.adds,
                                  "Explained"));

            done();
        });
    });

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

    describe('with body, without key, as buffers', function() {
        it('calls back with successful results', function(done) {
            var resp = new DtUpdateResp();
            resp.setHllValue(42);

            var callback = function(err, response) {
                assert(!err);
                assert(response);

                assert.equal(response.generatedKey, null);
                assert.equal(response.cardinality, 42);
                done();
            };

            var update = builder.
                    withCallback(callback).
                    withAdditions([jokesBuffer]).
                    build();

            update.onSuccess(resp);
        });
    });

    describe('with body, without key, as strings', function() {
        it('calls back with successful results', function(done) {
            var resp = new DtUpdateResp();
            resp.setHllValue(42);

            var callback = function(err, response) {
                assert(!err);
                assert(response);

                assert.equal(response.generatedKey, null);
                assert.equal(response.cardinality, 42);
                done();
            };

            var update = builder.
                    withCallback(callback).
                    withAdditions([someBuffer]).
                    build();

            update.onSuccess(resp);
        });
    });

    describe('without body, with key', function() {
        it('calls back with successful results', function(done) {
            var resp = new DtUpdateResp();
            resp.key = new Buffer("hll_2");

            var callback = function(err, response) {
                assert.equal(response.generatedKey, "hll_2");
                done();
            };

            var update = builder.
                    withCallback(callback).
                    withAdditions([jokesBuffer]).
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

    describe('additions are required', function() {
        it('throws an error if an empty array of additions are made', function(done) {
            var validationError = null;
            try {
              builder.withAdditions([]).build();
            } catch(err) {
                validationError = err;
            }
            assert.notEqual(validationError, null);
            done();
        });

        it('throws an error if no additions are made', function(done) {
            var validationError = null;
            try {
              builder.build();
            } catch(err) {
                validationError = err;
            }
            assert.notEqual(validationError, null);
            done();
        });
    });
});
