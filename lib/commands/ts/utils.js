'use strict';

var ByteBuffer = require('bytebuffer');
var Long = require('long');
var util = require('util');

var utils = require('../../utils');
var rpb = require('../../protobuf/riakprotobuf');
var TsCell = rpb.getProtoFor('TsCell');
var TsColumnType = rpb.getProtoFor('TsColumnType');

function isFloat(n) {
    return typeof n === 'number' && n % 1 !== 0;
}

function isInteger(n) {
    if (n instanceof Long) {
        return true;
    } else {
        return typeof n === 'number' && n % 1 === 0;
    }
}

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
                tsc.setBinaryValue(cell);
            } else if (ByteBuffer.isByteBuffer(cell)) {
                tsc.setBinaryValue(cell.toBuffer());
            } else if (isDate(cell)) {
                tsc.setTimestampValue(maybeConvertToLong(cell));
            } else if (isBoolean(cell)) {
                tsc.setBooleanValue(cell);
            } else if (utils.isString(cell)) { // NB: should come before isInteger / isFloat
                tsc.setBinaryValue(new Buffer(cell, 'utf8'));
            } else if (isInteger(cell)) {
                tsc.setSint64Value(cell);
            } else if (isFloat(cell)) {
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
                var rsprow = [];
                var rcells = rrows[i].getCells();
                for (var j = 0; j < rcells.length; j++) {
                    var rcol = rcols[j];
                    var rcell = rcells[j];
                    if (!rcell) {
                        rsprow.push(null);
                        continue;
                    }
                    var coltype = rcol.getType();
                    switch (coltype) {
                        case TsColumnType.BINARY:
                            var binaryValue = rcell.getBinaryValue();
                            if (binaryValue) {
                                // NB: responses will have Buffers for BINARY
                                rsprow.push(binaryValue.toBuffer());
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
                            var value = rcell.getTimestampValue();
                            if (!value) {
                                value = rcell.getSint64Value();
                            }
                            rsprow.push(value);
                            break;
                        case TsColumnType.BOOLEAN:
                            rsprow.push(rcell.getBooleanValue());
                            break;
                        default:
                            throw new Error('unknown column type: ' + coltype);
                    }
                }
                response.rows.push(rsprow);
            }
        }
    }

    return response;
}

module.exports.maybeConvertToLong = maybeConvertToLong;
module.exports.convertToTsCells = convertToTsCells;
module.exports.convertToResponse = convertToResponse;
