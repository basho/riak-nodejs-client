var TS = require('../../../lib/commands/ts');

var rpb = require('../../../lib/protobuf/riakprotobuf');
var TsColumnDescription = rpb.getProtoFor('TsColumnDescription');
var TsQueryResp = rpb.getProtoFor('TsQueryResp');
var TsGetResp = rpb.getProtoFor('TsGetResp');
var TsColumnType = rpb.getProtoFor('TsColumnType');
var TsRow = rpb.getProtoFor('TsRow');
var TsCell = rpb.getProtoFor('TsCell');

var assert = require('assert');
var crypto = require('crypto');
var logger = require('winston');
var Long = require('long');

var columns = [
    { name: 'col_binary',    type: TS.ColumnType.Binary },
    { name: 'col_int64',     type: TS.ColumnType.Int64 },
    { name: 'col_double',    type: TS.ColumnType.Double },
    { name: 'col_timestamp', type: TS.ColumnType.Timestamp },
    { name: 'col_boolean',   type: TS.ColumnType.Boolean },
    { name: 'col_ms',        type: TS.ColumnType.Timestamp }
];

var rpbcols = [];
columns.forEach(function (col) {
    var tcd = new TsColumnDescription();
    tcd.setName(new Buffer(col.name));
    tcd.setType(col.type);
    rpbcols.push(tcd);
});

module.exports.columns = columns;

var bd0 = crypto.randomBytes(16);
module.exports.bd0 = bd0;
var bd1 = crypto.randomBytes(16);
module.exports.bd1 = bd1;

var ts0 = new Date();
var ts0ms = Long.fromNumber(ts0.getTime());
module.exports.ts0 = ts0;
module.exports.ts0ms = ts0ms;

var ts1 = new Date();
var ts1ms = Long.fromNumber(ts1.getTime());
module.exports.ts1 = ts1;
module.exports.ts1ms = ts1ms;

var rows = [
    [ bd0, 0, 1.2, ts0, true, ts0ms ],
    [ bd1, 3, 4.5, ts1, false, ts1ms ],
    [ null, 6, 7.8, null, false, null ]
];
module.exports.rows = rows;

var rpbrows = [];
for (var i = 0; i < rows.length; i++) {
    var tsr = new TsRow();
    var row = rows[i];
    for (var j = 0; j < row.length; j++) {
        var cell = new TsCell();
        var val = row[j];
        switch (j) {
            case TS.ColumnType.Binary:
                cell.setBinaryValue(val);
                break;
            case TS.ColumnType.Int64:
                cell.setSint64Value(val);
                break;
            case TS.ColumnType.Double:
                cell.setDoubleValue(val);
                break;
            case TS.ColumnType.Timestamp:
                if (val) {
                    cell.setTimestampValue(Long.fromNumber(val.getTime()));
                }
                break;
            case TS.ColumnType.Boolean:
                cell.setBooleanValue(val);
                break;
            case 5:
                cell.setTimestampValue(val);
                break;
            default:
                throw new Error('huh?');
        }
        tsr.cells.push(cell);
    }
    rpbrows.push(tsr);
}

/*
for (var i = 0; i < rpbrows.length; i++) {
    logger.debug("RPBROW", i, ":");
    var cells = rpbrows[i].getCells();
    for (var j = 0; j < cells.length; j++) {
        logger.debug("    CELL", j, ":");
        logger.debug("        ", JSON.stringify(cells[j]));
    }
}
*/

var tsQueryResp = new TsQueryResp();
Array.prototype.push.apply(tsQueryResp.columns, rpbcols);
Array.prototype.push.apply(tsQueryResp.rows, rpbrows);
module.exports.tsQueryResp = tsQueryResp;

var tsGetResp = new TsGetResp();
Array.prototype.push.apply(tsGetResp.columns, rpbcols);
Array.prototype.push.apply(tsGetResp.rows, rpbrows);
module.exports.tsGetResp = tsGetResp;

function validateResponse(actual, expected) {
    assert(actual.columns, 'expected columns in response');
    assert(actual.rows, 'expected rows in response');

    var rc = actual.columns;
    assert.strictEqual(rc.length, expected.columns.length);
    assert.strictEqual(rc[0].name, 'col_binary');
    assert.strictEqual(rc[0].type, TsColumnType.BINARY);
    assert.strictEqual(rc[0].type, TS.ColumnType.Binary);
    assert.strictEqual(rc[1].name, 'col_int64');
    assert.strictEqual(rc[1].type, TsColumnType.SINT64);
    assert.strictEqual(rc[1].type, TS.ColumnType.Int64);
    assert.strictEqual(rc[2].name, 'col_double');
    assert.strictEqual(rc[2].type, TsColumnType.DOUBLE);
    assert.strictEqual(rc[2].type, TS.ColumnType.Double);
    assert.strictEqual(rc[3].name, 'col_timestamp');
    assert.strictEqual(rc[3].type, TsColumnType.TIMESTAMP);
    assert.strictEqual(rc[3].type, TS.ColumnType.Timestamp);
    assert.strictEqual(rc[4].name, 'col_boolean');
    assert.strictEqual(rc[4].type, TsColumnType.BOOLEAN);
    assert.strictEqual(rc[4].type, TS.ColumnType.Boolean);
    assert.strictEqual(rc[5].name, 'col_ms');
    assert.strictEqual(rc[5].type, TsColumnType.TIMESTAMP);
    assert.strictEqual(rc[5].type, TS.ColumnType.Timestamp);

    var rr = actual.rows;
    assert.strictEqual(rr.length, expected.rows.length);

    var r0 = rr[0];
    assert(r0[0] instanceof Buffer);
    assert(bd0.equals(r0[0]));
    assert(r0[1].equals(Long.ZERO));
    assert.strictEqual(r0[2], 1.2);
    assert(r0[3] instanceof Long);
    assert(ts0ms.equals(r0[3]));
    assert.strictEqual(r0[4], true);
    assert(ts0ms.equals(r0[5]));

    var r1 = rr[1];
    assert(r1[0] instanceof Buffer);
    assert(bd1.equals(r1[0]));
    assert(r1[1].equals(3));
    assert.strictEqual(r1[2], 4.5);
    assert(r1[3] instanceof Long);
    assert(ts1ms.equals(r1[3]));
    assert.strictEqual(r1[4], false);
    assert(ts1ms.equals(r1[5]));

    var r2 = rr[2];
    assert.strictEqual(r2[0], null);
    assert(r2[1].equals(6));
    assert.strictEqual(r2[2], 7.8);
    assert.strictEqual(r2[3], null);
    assert.strictEqual(r2[4], false);
    assert.strictEqual(r2[5], null);
}

module.exports.validateResponse = validateResponse;
