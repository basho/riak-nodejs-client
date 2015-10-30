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

    this.timeout(250);

    describe('Build', function() {
        it('should build a TsGetReq correctly', function(done) {
            var cmd = new TS.Get.Builder()
               .withTable(table)
               .withKey(key)
               .withCallback(function(){})
               .build();
            var protobuf = cmd.constructPbRequest();
            assert.strictEqual(protobuf.getTable().toString('utf8'), table);

            var tsc = protobuf.getKey();
            for (var i = 0; i < key.length; i++) {
                assert.strictEqual(tsc[i].toString('utf8'), key[i]);
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
           var cb = function(err, response) {
                assert(err, !err);
                assert.strictEqual(err, 'this is an error');
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
