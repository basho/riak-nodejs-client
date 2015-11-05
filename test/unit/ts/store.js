'use strict';

var d = require('./data');
var TS = require('../../../lib/commands/ts');

var rpb = require('../../../lib/protobuf/riakprotobuf');
var TsPutResp = rpb.getProtoFor('TsPutResp');
var TsColumnType = rpb.getProtoFor('TsColumnType');
var RpbErrorResp = rpb.getProtoFor('RpbErrorResp');

var assert = require('assert');
if (!assert.deepStrictEqual) {
    assert.deepStrictEqual = assert.deepEqual;
}

var crypto = require('crypto');
var logger = require('winston');
var Long = require('long');

function validateTsPutReq(protobuf, hasCols) {
    assert.strictEqual(protobuf.getTable().toString('utf8'), 'table');

    if (hasCols) {
        var pcols = protobuf.getColumns();
        assert.strictEqual(pcols.length, d.columns.length);

        var col0 = pcols[0];
        assert.strictEqual(col0.getName().toString('utf8'), 'col_varchar');
        assert.strictEqual(col0.getType(), TsColumnType.VARCHAR);

        var col1 = pcols[1];
        assert.strictEqual(col1.getName().toString('utf8'), 'col_int64');
        assert.strictEqual(col1.getType(), TsColumnType.SINT64);

        var col2 = pcols[2];
        assert.strictEqual(col2.getName().toString('utf8'), 'col_double');
        assert.strictEqual(col2.getType(), TsColumnType.DOUBLE);

        var col3 = pcols[3];
        assert.strictEqual(col3.getName().toString('utf8'), 'col_timestamp');
        assert.strictEqual(col3.getType(), TsColumnType.TIMESTAMP);

        var col4 = pcols[4];
        assert.strictEqual(col4.getName().toString('utf8'), 'col_boolean');
        assert.strictEqual(col4.getType(), TsColumnType.BOOLEAN);

        var col5 = pcols[5];
        assert.strictEqual(col5.getName().toString('utf8'), 'col_ms');
        assert.strictEqual(col5.getType(), TsColumnType.TIMESTAMP);
    }

    var prows = protobuf.getRows();
    assert.strictEqual(prows.length, d.rows.length);

    var row0 = prows[0];
    var row0cells = row0.getCells();

    assert(d.bd0.equals(row0cells[0].getVarcharValue().toBuffer()));
    assert(row0cells[1].getSint64Value().equals(Long.ZERO));
    assert.strictEqual(row0cells[2].getDoubleValue(), 1.2);
    // ts0 is a Date
    var r0c3tsv = row0cells[3].getTimestampValue();
    assert(d.ts0ms.equals(r0c3tsv));
    assert.strictEqual(row0cells[4].getBooleanValue(), true);

    var r0c5ms;
    if (hasCols) {
        r0c5ms = row0cells[5].getTimestampValue();
    } else {
        r0c5ms = row0cells[5].getSint64Value();
    }
    assert(Long.isLong(r0c5ms));
    assert(r0c5ms.equals(d.ts0ms));
    
    var row1 = prows[1];
    var row1cells = row1.getCells();
    var three = new Long(3);

    assert(d.bd1.equals(row1cells[0].getVarcharValue().toBuffer()));
    assert(row1cells[1].getSint64Value().equals(three));
    assert.strictEqual(row1cells[2].getDoubleValue(), 4.5);

    var r1c3tsv = row1cells[3].getTimestampValue();
    assert(d.ts1ms.equals(r1c3tsv));

    assert.strictEqual(row1cells[4].getBooleanValue(), false);

    var r1c5ms;
    if (hasCols) {
        r1c5ms = row0cells[5].getTimestampValue();
    } else {
        r1c5ms = row0cells[5].getSint64Value();
    }
    assert(Long.isLong(r1c5ms));
    assert(r1c5ms.equals(d.ts0ms));
}

describe('Store', function() {

    this.timeout(250);

    describe('General', function() {
        it('should convert a date to a Long', function(done) {
            var date = new Date();
            var millis = date.getTime();
            var l = Long.fromNumber(millis);
            assert.strictEqual(millis.toString(), l.toString());
            done();
        });
    });

    describe('Build', function() {
        it('should build a TsPutReq correctly', function(done) {
            var storeCommand = new TS.Store.Builder()
               .withTable('table')
               .withColumns(d.columns)
               .withRows(d.rows)
               .withCallback(function(){})
               .build();
            var protobuf = storeCommand.constructPbRequest();
            validateTsPutReq(protobuf, true);
            done();
        });

        it('should build a TsPutReq correctly without column information', function(done) {
            var storeCommand = new TS.Store.Builder()
               .withTable('table')
               .withRows(d.rows)
               .withCallback(function(){})
               .build();
            var protobuf = storeCommand.constructPbRequest();
            validateTsPutReq(protobuf, false);
            done();
        });
        
        it('should take a TsPutResp and call the users callback with the response', function(done) {
            var tsPutResp = new TsPutResp();
            
            var cb = function(err, response) {
                assert(response === true);
                done();
            };

            var storeCommand = new TS.Store.Builder()
               .withTable('table')
               .withColumns(d.columns)
               .withRows(d.rows)
               .withCallback(cb)
               .build();
       
            storeCommand.onSuccess(tsPutResp);
        });
        
        it ('should take a RpbErrorResp and call the users callback with the error message', function(done) {
           var rpbErrorResp = new RpbErrorResp();
           rpbErrorResp.setErrmsg(new Buffer('this is an error'));
           
           var cb = function(err, response) {
                assert(err, !err);
                assert.strictEqual(err, 'this is an error');
                done();
            };
           
            var storeCommand = new TS.Store.Builder()
               .withTable('table')
               .withColumns(d.columns)
               .withRows(d.rows)
               .withCallback(cb)
               .build();
       
            storeCommand.onRiakError(rpbErrorResp);
        });
    });
});
