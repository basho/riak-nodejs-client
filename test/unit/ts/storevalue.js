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
        assert.strictEqual(col0.getName().toString('utf8'), 'col_binary');
        assert.strictEqual(col0.getType(), TsColumnType.BINARY);

        var col1 = pcols[1];
        assert.strictEqual(col1.getName().toString('utf8'), 'col_int');
        assert.strictEqual(col1.getType(), TsColumnType.INTEGER);

        var col2 = pcols[2];
        assert.strictEqual(col2.getName().toString('utf8'), 'col_numeric');
        assert.strictEqual(col2.getType(), TsColumnType.NUMERIC);

        var col3 = pcols[3];
        assert.strictEqual(col3.getName().toString('utf8'), 'col_timestamp');
        assert.strictEqual(col3.getType(), TsColumnType.TIMESTAMP);

        var col4 = pcols[4];
        assert.strictEqual(col4.getName().toString('utf8'), 'col_boolean');
        assert.strictEqual(col4.getType(), TsColumnType.BOOLEAN);

        var col5 = pcols[5];
        assert.strictEqual(col5.getName().toString('utf8'), 'col_set');
        assert.strictEqual(col5.getType(), TsColumnType.SET);

        var col6 = pcols[6];
        assert.strictEqual(col6.getName().toString('utf8'), 'col_map');
        assert.strictEqual(col6.getType(), TsColumnType.MAP);

        var col7 = pcols[7];
        assert.strictEqual(col7.getName().toString('utf8'), 'col_ms');
        assert.strictEqual(col7.getType(), TsColumnType.TIMESTAMP);
    }

    var prows = protobuf.getRows();
    assert.strictEqual(prows.length, d.rows.length);

    var row0 = prows[0];
    var row0cells = row0.getCells();

    assert(d.bd0.equals(row0cells[0].getBinaryValue().toBuffer()));
    assert(row0cells[1].getIntegerValue().equals(Long.ZERO));
    assert.strictEqual(row0cells[2].getNumericValue().toString('utf8'), '1.2');
    // ts0 is a Date
    var r0c3tsv = row0cells[3].getTimestampValue();
    assert(d.ts0ms.equals(r0c3tsv));
    assert.strictEqual(row0cells[4].getBooleanValue(), true);

    // setvalue is an array of buffers
    var s0 = [];
    row0cells[5].getSetValue().forEach(function (buf) {
        s0.push(JSON.parse(buf.toString('utf8')));
    });
    assert.deepStrictEqual(s0, d.set);

    var mapval0 = JSON.parse(row0cells[6].getMapValue().toString('utf8'));
    assert.deepStrictEqual(mapval0, d.map);

    var r0c7ms;
    if (hasCols) {
        r0c7ms = row0cells[7].getTimestampValue();
    } else {
        r0c7ms = row0cells[7].getIntegerValue();
    }
    assert(Long.isLong(r0c7ms));
    assert(r0c7ms.equals(d.ts0ms));
    
    var row1 = prows[1];
    var row1cells = row1.getCells();
    var three = new Long(3);

    assert(d.bd1.equals(row1cells[0].getBinaryValue().toBuffer()));
    assert(row1cells[1].getIntegerValue().equals(three));
    assert.strictEqual(row1cells[2].getNumericValue().toString('utf8'), '4.5');

    var r1c3tsv = row1cells[3].getTimestampValue();
    assert(d.ts1ms.equals(r1c3tsv));

    assert.strictEqual(row1cells[4].getBooleanValue(), false);

    // setvalue is an array of buffers
    var s1 = [];
    row1cells[5].getSetValue().forEach(function (buf) {
        s1.push(JSON.parse(buf.toString('utf8')));
    });
    assert.deepStrictEqual(s1, d.set);

    var mapval1 = JSON.parse(row1cells[6].getMapValue().toString('utf8'));
    assert.deepStrictEqual(mapval1, d.map);

    var r1c7ms;
    if (hasCols) {
        r1c7ms = row0cells[7].getTimestampValue();
    } else {
        r1c7ms = row0cells[7].getIntegerValue();
    }
    assert(Long.isLong(r1c7ms));
    assert(r1c7ms.equals(d.ts0ms));
}

describe('StoreValue', function() {

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
            var storeCommand = new TS.StoreValue.Builder()
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
            var storeCommand = new TS.StoreValue.Builder()
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

            var storeCommand = new TS.StoreValue.Builder()
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
           
            var storeCommand = new TS.StoreValue.Builder()
               .withTable('table')
               .withColumns(d.columns)
               .withRows(d.rows)
               .withCallback(cb)
               .build();
       
            storeCommand.onRiakError(rpbErrorResp);
        });
    });
});
