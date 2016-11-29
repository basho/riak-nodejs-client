'use strict';

var ByteBuffer = require('bytebuffer');
var Long = require('long');
var util = require('util');

var utils = require('../../utils');
var rpb = require('../../protobuf/riakprotobuf');
var TsCell = rpb.getProtoFor('TsCell');
var TsColumnType = rpb.getProtoFor('TsColumnType');

function isBoolean(v) {
    return (typeof v === 'boolean' || v instanceof Boolean);
}

function isDate(v) {
    return v instanceof Date;
}

function isObject(val) {
    if (val === null) {
        return false;
    }
    return ((typeof val === 'function') || (typeof val === 'object'));
}

function maybeConvertToLong(v) {
    if (Long.isLong(v)) {
        return v;
    }
    else if (isDate(v)) {
        return Long.fromNumber(v.getTime());
    } else {
        return v;
    }
}

function convertToTsCells(row) {
    // Guess type to populate cells
    var cells = [];
    row.forEach(function (cell) {
        var tsc = new TsCell();
        if (!utils.isNullOrUndefined(cell)) {
            if (Buffer.isBuffer(cell)) {
                tsc.setVarcharValue(cell);
            } else if (ByteBuffer.isByteBuffer(cell)) {
                tsc.setVarcharValue(cell.toBuffer());
            } else if (isDate(cell)) {
                tsc.setTimestampValue(maybeConvertToLong(cell));
            } else if (isBoolean(cell)) {
                tsc.setBooleanValue(cell);
            } else if (utils.isString(cell)) { // NB: should come before isInteger / isFloat
                tsc.setVarcharValue(new Buffer(cell, 'utf8'));
            } else if (utils.isInteger(cell)) {
                tsc.setSint64Value(cell);
            } else if (utils.isFloat(cell)) {
                tsc.setDoubleValue(cell);
            } else {
                var msg = util.format("could not serialize: %s, type: %s",
                                    JSON.stringify(cell), typeof cell);
                throw new Error(msg);
            }
        }
        cells.push(tsc);
    });
    return cells;
}

function convertTsRow(tsRow, tsCols) {
    var rsprow = [];
    var rcells = tsRow.getCells();
    for (var j = 0; j < rcells.length; j++) {
        var rcell = rcells[j];
        if (!rcell) {
            rsprow.push(null);
            continue;
        }
        if (tsCols) {
            var rcol = tsCols[j];
            var coltype = rcol.getType();
            switch (coltype) {
                case TsColumnType.VARCHAR:
                case TsColumnType.BLOB:
                    var varcharValue = rcell.getVarcharValue();
                    if (varcharValue) {
                        // NB: responses will have ByteBuffers for VARCHAR
                        rsprow.push(varcharValue.toBuffer());
                    } else {
                        rsprow.push(null);
                    }
                    break;
                case TsColumnType.SINT64:
                    rsprow.push(rcell.getSint64Value());
                    break;
                case TsColumnType.DOUBLE:
                    rsprow.push(rcell.getDoubleValue());
                    break;
                case TsColumnType.TIMESTAMP:
                    rsprow.push(rcell.getTimestampValue());
                    break;
                case TsColumnType.BOOLEAN:
                    rsprow.push(rcell.getBooleanValue());
                    break;
                default:
                    throw new Error('unknown column type: ' + coltype);
            }
        } else {
            var val = rcell.getVarcharValue();
            if (val) {
                // NB: responses will have ByteBuffers for VARCHAR
                rsprow.push(val.toBuffer());
                continue;
            }
            val = rcell.getSint64Value();
            if (val) {
                rsprow.push(val);
                continue;
            }
            val = rcell.getDoubleValue();
            if (val) {
                rsprow.push(val);
                continue;
            }
            val = rcell.getTimestampValue();
            if (val) {
                rsprow.push(val);
                continue;
            }
            val = rcell.getBooleanValue();
            if (val) {
                rsprow.push(val);
                continue;
            }
            rsprow.push(null);
        }
    }
    return rsprow;
}

function convertToResponse(rpbResp) {
    var response = {
        columns: [],
        rows: [],
    };

    if (rpbResp) {
        var rcols = rpbResp.getColumns();
        if (rcols && rcols.length > 0) {
            rcols.forEach(function (rc) {
                var col = {
                    name: rc.getName().toString('utf8'),
                    type: rc.getType()
                };
                response.columns.push(col);
            });
        }
        var rrows = rpbResp.getRows();
        if (rrows && rrows.length > 0) {
            for (var i = 0; i < rrows.length; i++) {
                response.rows.push(convertTsRow(rrows[i], rcols));
            }
        }
    }

    return response;
}

module.exports.maybeConvertToLong = maybeConvertToLong;
module.exports.convertToTsCells = convertToTsCells;
module.exports.convertTsRow = convertTsRow;
module.exports.convertToResponse = convertToResponse;
