var FetchSet = require('../../lib/commands/crdt/fetchset');
var Rpb = require('../../lib/protobuf/riakprotobuf');
var DtFetchReq = Rpb.getProtoFor('DtFetchReq');
var DtFetchResp = Rpb.getProtoFor('DtFetchResp');

var assert = require('assert');

describe('FetchSet', function() {
    describe('Build', function() {
        it('builds a DtFetchSet correctly', function(done){
            var fetchSet = new FetchSet.Builder().
                    withBucketType('sets_type').
                    withBucket('set_bucket').
                    withKey('cool_set').
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
    });
});
