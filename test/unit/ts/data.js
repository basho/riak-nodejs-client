var TS = require('../../../lib/commands/ts');

var rpb = require('../../../lib/protobuf/riakprotobuf');
var TsColumnDescription = rpb.getProtoFor('TsColumnDescription');
var TsQueryResp = rpb.getProtoFor('TsQueryResp');
var TsRow = rpb.getProtoFor('TsRow');
var TsCell = rpb.getProtoFor('TsCell');

var crypto = require('crypto');
var logger = require('winston');
var Long = require('long');

var columns = [
    { name: 'col_binary',    type: TS.ColumnType.Binary },
    { name: 'col_int',       type: TS.ColumnType.Integer },
    { name: 'col_numeric',   type: TS.ColumnType.Numeric },
    { name: 'col_timestamp', type: TS.ColumnType.Timestamp },
    { name: 'col_boolean',   type: TS.ColumnType.Boolean },
    { name: 'col_set',       type: TS.ColumnType.Set },
    { name: 'col_map',       type: TS.ColumnType.Map },
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

var set = ['foo', 'bar', 'baz'];
module.exports.set = set;

var map = {
    foo: 'foo',
    bar: 'bar',
    baz: 'baz',
    set: set
};
module.exports.map = map;

var rows = [
    [ bd0, 0, 1.2, ts0, true, set, map, ts0ms ],
    [ bd1, 3, 4.5, ts1, false, set, map, ts1ms ],
    [ null, 6, 7.8, null, false, null, undefined, null ]
];
module.exports.rows = rows;

function makeSetValueSerializer(tsc) {
    return function (cv) {
        tsc.set_value.push(new Buffer(JSON.stringify(cv)));
    };
}

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
            case TS.ColumnType.Integer:
                cell.setIntegerValue(val);
                break;
            case TS.ColumnType.Numeric:
                if (val) {
                    cell.setNumericValue(new Buffer(val.toString()));
                }
                break;
            case TS.ColumnType.Timestamp:
                if (val) {
                    cell.setTimestampValue(Long.fromNumber(val.getTime()));
                }
                break;
            case TS.ColumnType.Boolean:
                cell.setBooleanValue(val);
                break;
            case TS.ColumnType.Set:
                if (val) {
                    var setValueToJSON = makeSetValueSerializer(cell);
                    val.forEach(setValueToJSON);
                }
                break;
            case TS.ColumnType.Map:
                if (val) {
                    var json = JSON.stringify(val);
                    cell.setMapValue(new Buffer(json));
                }
                break;
            case 7:
                cell.setTimestampValue(val);
                break;
            default:
                throw new Error('huh?');
        }
        tsr.cells.push(cell);
    }
    rpbrows.push(tsr);
}

var tsQueryResp = new TsQueryResp();
Array.prototype.push.apply(tsQueryResp.columns, rpbcols);
Array.prototype.push.apply(tsQueryResp.rows, rpbrows);
module.exports.tsQueryResp = tsQueryResp;
