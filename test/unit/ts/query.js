var d = require('./data');
var TS = require('../../../lib/commands/ts');

var rpb = require('../../../lib/protobuf/riakprotobuf');
var TsQueryReq = rpb.getProtoFor('TsQueryReq');
var TsQueryResp = rpb.getProtoFor('TsQueryResp');
var TsColumnType = rpb.getProtoFor('TsColumnType');
var RpbErrorResp = rpb.getProtoFor('RpbErrorResp');

var assert = require('assert');
if (!assert.deepStrictEqual) {
    assert.deepStrictEqual = assert.deepEqual;
}

var logger = require('winston');
var ByteBuffer = require('bytebuffer');
var Long = require('long');

var queryText = 'select * from foo where baz = "bat"';

describe('Query', function() {

    this.timeout(250);

    describe('Build', function() {
        it('should build a TsQueryReq correctly', function(done) {
            var queryCommand = new TS.Query.Builder()
               .withQuery(queryText)
               .withCallback(function(){})
               .build();
            var protobuf = queryCommand.constructPbRequest();

            var tsi = protobuf.getQuery();
            assert(tsi, 'expected Tsinterpolation');

            var base = tsi.getBase().toString('utf8');
            assert.strictEqual(base, queryText);

            var i = tsi.getInterpolations();
            assert(Array.isArray(i));
            assert.strictEqual(i.length, 0);

            done();
        });
        
        it('should take a TsQueryResp and call the users callback with the response', function(done) {

            var tsQueryResp = d.tsQueryResp;

            var cb = function(err, response) {
                assert(!err, err);
                assert(response.columns, 'expected columns in response');
                assert(response.rows, 'expected rows in response');

                var rc = response.columns;
                assert.strictEqual(rc.length, tsQueryResp.columns.length);
                assert.strictEqual(rc[0].name, 'col_binary');
                assert.strictEqual(rc[0].type, TsColumnType.BINARY);
                assert.strictEqual(rc[0].type, TS.ColumnType.Binary);
                assert.strictEqual(rc[1].name, 'col_int');
                assert.strictEqual(rc[1].type, TsColumnType.INTEGER);
                assert.strictEqual(rc[1].type, TS.ColumnType.Integer);
                assert.strictEqual(rc[2].name, 'col_numeric');
                assert.strictEqual(rc[2].type, TsColumnType.NUMERIC);
                assert.strictEqual(rc[2].type, TS.ColumnType.Numeric);
                assert.strictEqual(rc[3].name, 'col_timestamp');
                assert.strictEqual(rc[3].type, TsColumnType.TIMESTAMP);
                assert.strictEqual(rc[3].type, TS.ColumnType.Timestamp);
                assert.strictEqual(rc[4].name, 'col_boolean');
                assert.strictEqual(rc[4].type, TsColumnType.BOOLEAN);
                assert.strictEqual(rc[4].type, TS.ColumnType.Boolean);
                assert.strictEqual(rc[5].name, 'col_set');
                assert.strictEqual(rc[5].type, TsColumnType.SET);
                assert.strictEqual(rc[5].type, TS.ColumnType.Set);
                assert.strictEqual(rc[6].name, 'col_map');
                assert.strictEqual(rc[6].type, TsColumnType.MAP);
                assert.strictEqual(rc[6].type, TS.ColumnType.Map);
                assert.strictEqual(rc[7].name, 'col_ms');
                assert.strictEqual(rc[7].type, TsColumnType.TIMESTAMP);
                assert.strictEqual(rc[7].type, TS.ColumnType.Timestamp);

                var rr = response.rows;
                assert.strictEqual(tsQueryResp.rows.length, rr.length);

                var r0 = rr[0];
                assert(r0[0] instanceof ByteBuffer);
                assert(d.bd0.equals(r0[0].toBuffer()));

                assert(r0[1].equals(Long.ZERO));

                assert.strictEqual(r0[2], '1.2');

                assert(r0[3] instanceof Long);
                assert(d.ts0ms.equals(r0[3]));

                assert.strictEqual(r0[4], true);

                assert.deepStrictEqual(r0[5], d.set);

                assert.deepStrictEqual(r0[6], d.map);

                assert(d.ts0ms.equals(r0[7]));

                var r1 = rr[1];
                assert(r1[0] instanceof ByteBuffer);
                assert(d.bd1.equals(r1[0].toBuffer()));

                assert(r1[1].equals(3));

                assert.strictEqual(r1[2], '4.5');

                assert(r1[3] instanceof Long);
                assert(d.ts1ms.equals(r1[3]));

                assert.strictEqual(r1[4], false);

                assert.deepStrictEqual(r1[5], d.set);

                assert.deepStrictEqual(r1[6], d.map);

                assert(d.ts1ms.equals(r1[7]));

                done();
            };

            var queryCommand = new TS.Query.Builder()
               .withQuery(queryText)
               .withCallback(cb)
               .build();
       
            queryCommand.onSuccess(tsQueryResp);
        });
        
        it ('should take a RpbErrorResp and call the users callback with the error message', function(done) {
           var rpbErrorResp = new RpbErrorResp();
           rpbErrorResp.setErrmsg(new Buffer('this is an error'));
           
           var cb = function(err, response) {
                assert(err, !err);
                assert.strictEqual(err, 'this is an error');
                done();
            };
           
            var queryCommand = new TS.Query.Builder()
               .withQuery(queryText)
               .withCallback(cb)
               .build();
       
            queryCommand.onRiakError(rpbErrorResp);
        });
    });
});
