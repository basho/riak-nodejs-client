'use strict';

var FetchSet = require('../../../lib/commands/crdt/fetchset');
var Rpb = require('../../../lib/protobuf/riakprotobuf');
var DtFetchReq = Rpb.getProtoFor('DtFetchReq');
var DtFetchResp = Rpb.getProtoFor('DtFetchResp');
var DtValue = Rpb.getProtoFor('DtValue');
var RpbErrorResp = Rpb.getProtoFor('RpbErrorResp');
var ByteBuffer = require('bytebuffer');

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

        it('calls back with successful results as buffers', function(done){
            var resp = new DtFetchResp();
            resp.type = 2;
            resp.context = ByteBuffer.fromUTF8("asdf");

            var value = new DtValue();
            resp.setValue(value);

            value.set_value = [ByteBuffer.fromUTF8("zedo"),
                               ByteBuffer.fromUTF8("piper"),
                               ByteBuffer.fromUTF8("little one")];

            var includesBuffer = function(haystack, needle) {
                var needleBuf = new Buffer(needle);
                var len = haystack.length;
                for (var i = 0; i < len; i++) {
                    if (haystack[i].equals(needleBuf)) return true;
                }
                
                return false;
            };

            var callback = function(err, response) {
                assert.equal(response.context.toString("utf8"), "asdf");

                assert(includesBuffer(response.values, "zedo"));

                assert(includesBuffer(response.values, "piper"));
                
                assert(includesBuffer(response.values, "little one"));
                
                done();
            };

            var fetch = builder.
                    withCallback(callback).
                    withSetsAsBuffers(true).
                    build();

            fetch.onSuccess(resp);
        });
        
        it('calls back with successful results as strings', function(done){
            var resp = new DtFetchResp();
            resp.type = 2;
            resp.context = ByteBuffer.fromUTF8("asdf");

            var value = new DtValue();
            resp.setValue(value);

            value.set_value = [ByteBuffer.fromUTF8("zedo"),
                               ByteBuffer.fromUTF8("piper"),
                               ByteBuffer.fromUTF8("little one")];

            var includes = function(haystack, needle) {
                var len = haystack.length;
                for (var i = 0; i < len; i++) {
                    if (haystack[i] === needle) return true;
                }

                return false;
            };
            
            var callback = function(err, response) {
                assert.equal(response.context.toString("utf8"), "asdf");

                assert(includes(response.values, "zedo"));

                assert(includes(response.values, "piper"));
                
                assert(includes(response.values, "little one"));
                
                done();
            };

            var fetch = builder.
                    withCallback(callback).
                    withSetsAsBuffers(false).
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
