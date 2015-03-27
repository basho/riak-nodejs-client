var UpdateSet = require('../../../lib/commands/crdt/updateset');
var Rpb = require('../../../lib/protobuf/riakprotobuf');
var DtUpdateReq = Rpb.getProtoFor('DtUpdateReq');
var DtUpdateResp = Rpb.getProtoFor('DtUpdateResp');
var DtValue = Rpb.getProtoFor('DtValue');
var RpbErrorResp = Rpb.getProtoFor('RpbErrorResp');

var ByteBuffer = require('bytebuffer');
var assert = require('assert');

describe('UpdateSet', function() {
    describe('Build', function() {
        var builder = new UpdateSet.Builder().
                withBucketType('sets_type').
                withBucket('set_bucket').
                withKey('cool_set');

        // ikea rugs
        var hampenBuffer = ByteBuffer.fromUTF8("hampen");
        var snabbfotadBuffer = ByteBuffer.fromUTF8("snabbfotad");

        var someContext = ByteBuffer.fromUTF8("context");

        var includesBuffer = function(haystack, needle) {
            var len = haystack.length;
            for (var i = 0; i < len; i++) {
                if (haystack[i].toString('utf8') === needle) {
                    return true;
                }
            }

            return false;
        };
        
        it('builds a DtUpdateSet correctly', function(done){
            var update = builder.
                    withContext(someContext).
                    withAdditions(["gåser", hampenBuffer]).
                    withRemovals([snabbfotadBuffer, "valby ruta"]).
                    withCallback(function(){}).
                    withW(1).
                    withDw(2).
                    withPw(3).
                    withReturnBody(false).
                    withTimeout(12345).
                    build();

            var protobuf = update.constructPbRequest();

            assert.equal(protobuf.getType().toString('utf8'),
                         'sets_type');
            assert.equal(protobuf.getBucket().toString('utf8'),
                         'set_bucket');
            assert.equal(protobuf.getKey().toString('utf8'),
                         'cool_set');
            assert.equal(protobuf.getW(), 1);
            assert.equal(protobuf.getDw(), 2);
            assert.equal(protobuf.getPw(), 3);

            assert.equal(protobuf.getReturnBody(), false);
            assert.equal(protobuf.getTimeout(), 12345);
            
            assert(includesBuffer(protobuf.op.set_op.adds,
                                      "gåser"));
            assert(includesBuffer(protobuf.op.set_op.adds,
                                      "hampen"));

            assert(includesBuffer(protobuf.op.set_op.removes,
                                      "snabbfotad"));
            assert(includesBuffer(protobuf.op.set_op.removes,
                                      "valby ruta"));

            done();
        });
    });
});
