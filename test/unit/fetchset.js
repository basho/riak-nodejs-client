var FetchSet = require('../../lib/commands/crdt/fetchset');
var Rpb = require('../../lib/protobuf/riakprotobuf');
var DtFetchReq = Rpb.getProtoFor('DtFetchReq');
var DtFetchResp = Rpb.getProtoFor('DtFetchResp');
var DtValue = Rpb.getProtoFor('DtValue');

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
                    build();

            var protobuf = fetchSet.constructPbRequest();

            assert.equal(protobuf.getType().toString('utf8'),
                         'sets_type');
            assert.equal(protobuf.getBucket().toString('utf8'),
                         'set_bucket');
            assert.equal(protobuf.getKey().toString('utf8'),
                         'cool_set');
            done();
        });

        it('calls a callback with results', function(done){
            var resp = new DtFetchResp();
            resp.type = 2;
            resp.context = new Buffer("asdf");

            var value = new DtValue();
            resp.setValue(value);

            value.set_value = ["zedo", "piper", "little one"];

            var callback = function(err, response) {
                assert.equal(response.context.toString("utf8"), "asdf");
                assert.equal(response.dataType, 2);
                assert.notEqual(response.value.indexOf("zedo"), -1);
                assert.notEqual(response.value.indexOf("piper"), -1);
                assert.notEqual(response.value.indexOf("little one"), -1);

                done();
            };

            var fetch = builder.
                    withCallback(callback).
                    build();

            fetch.onSuccess(resp);
        });
    });
});
