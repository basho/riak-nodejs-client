var tsdata = require('./data');
var CommandBase = require('../commandbase');

var inherits = require('util').inherits;
var Joi = require('joi');
var logger = require('winston');
var ByteBuffer = require('bytebuffer');
var Long = require('long');

var utils = require('../../utils');
var rpb = require('../../protobuf/riakprotobuf');
var TsColumnType = rpb.getProtoFor('TsColumnType');
var TsColumnDescription = rpb.getProtoFor('TsColumnDescription');
var TsRow = rpb.getProtoFor('TsRow');
var TsCell = rpb.getProtoFor('TsCell');

/**
 * Provides the StoreValue class, its builder, and its response.
 * @module TS
 */

/**
 * Command used to store timeseries data in Riak.
 *
 * As a convenience, a builder class is provided:
 *
 *      var storeValue = new StoreValue.Builder()
 *          .withTable('table')
 *          .withColumns(cols)
 *          .withRows(rows)
 *          .build();
 *
 * See {{#crossLink "StoreValue.Builder"}}StoreValue.Builder{{/crossLink}}
 *
 * @class StoreValue
 * @constructor
 * @param {Object} options The options for this command.
 * @param {String} options.table The timeseries table in Riak.
 * @param {Object[]} options.columns The timeseries columns in Riak.
 * @param {Object[]} options.rows The timeseries rows in Riak.
 * @param {Function} callback The allback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response Will be either true or false.
 * @extends CommandBase
 */
function StoreValue(options, callback) {
    CommandBase.call(this, 'TsPutReq', 'TsPutResp', callback);
    var self = this;
    Joi.validate(options, schema, function(err, options) {
        if (err) {
            throw err;
        }
        self.options = options;
    });
    this.remainingTries = 1;
}

inherits(StoreValue, CommandBase);

function isFloat(n) {
    return n === +n && n !== (n|0);
}

function isInteger(n) {
    if (n instanceof Long) {
        return true;
    } else {
        return n === +n && n === (n|0);
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

function makeSetValueSerializer(tsc) {
    return function (cv) {
        tsc.set_value.push(new Buffer(JSON.stringify(cv)));
    };
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

StoreValue.prototype.constructPbRequest = function() {
    var protobuf = this.getPbReqBuilder();
    protobuf.setTable(new Buffer(this.options.table));

    var i, ct;
    if (this.options.columns) {
        // Use column descriptions to populate cells
        var columnTypes = [];
        for (i = 0; i < this.options.columns.length; i++) {
            var col = this.options.columns[i];

            var tscd = new TsColumnDescription();
            tscd.setName(new Buffer(col.name));

            ct = col.type;
            columnTypes.push(ct);
            tscd.setType(ct);

            protobuf.columns.push(tscd);
        }

        for (i = 0; i < this.options.rows.length; i++) {
            var tsr = new TsRow();
            var row = this.options.rows[i];
            for (var j = 0; j < row.length; j++) {
                var tsc = new TsCell();
                ct = columnTypes[j];
                var cell = row[j];
                switch (ct) {
                    case TsColumnType.BINARY:
                        if (cell) {
                            if (Buffer.isBuffer(cell)) {
                                tsc.setBinaryValue(cell);
                            } else if (ByteBuffer.isByteBuffer(cell)) {
                                tsc.setBinaryValue(cell.toBuffer());
                            } else {
                                tsc.setBinaryValue(new Buffer(cell));
                            }
                        }
                        break;
                    case TsColumnType.INTEGER:
                        // NB: must be convertible to Long to serialize as sint64
                        tsc.setIntegerValue(cell);
                        break;
                    case TsColumnType.NUMERIC:
                        if (cell) {
                            tsc.setNumericValue(new Buffer(cell.toString()));
                        }
                        break;
                    case TsColumnType.TIMESTAMP:
                        // NB: must be convertible to Long to serialize as sint64
                        if (cell) {
                            var val = maybeConvertToLong(cell);
                            tsc.setTimestampValue(val);
                        }
                        break;
                    case TsColumnType.BOOLEAN:
                        tsc.setBooleanValue(cell);
                        break;
                    case TsColumnType.SET:
                        if (cell) {
                            if (Array.isArray(cell)) {
                                var setValueToJSON = makeSetValueSerializer(tsc);
                                cell.forEach(setValueToJSON);
                            } else {
                                tsc.set_value.push(new Buffer(JSON.stringify(cell)));
                            }
                        }
                        break;
                    case TsColumnType.MAP:
                        if (cell) {
                            tsc.setMapValue(new Buffer(JSON.stringify(cell)));
                        }
                        break;
                }
                tsr.cells.push(tsc);
            }
            protobuf.rows.push(tsr);
        }
    } else {
        // Guess type to populate cells
        this.options.rows.forEach(function (row) {
            var tsr = new TsRow();
            row.forEach(function (cell) {
                var tsc = new TsCell();
                if (cell) {
                    if (Buffer.isBuffer(cell)) {
                        tsc.setBinaryValue(cell);
                    } else if (ByteBuffer.isByteBuffer(cell)) {
                        tsc.setBinaryValue(cell.toBuffer());
                    } else if (isDate(cell)) {
                        tsc.setTimestampValue(maybeConvertToLong(cell));
                    } else if (Array.isArray(cell)) {
                        var setValueToJSON = makeSetValueSerializer(tsc);
                        cell.forEach(setValueToJSON);
                    } else if (isBoolean(cell)) {
                        tsc.setBooleanValue(cell);
                    } else if (utils.isString(cell)) { // NB: should come before isInteger / isFloat
                        tsc.setBinaryValue(new Buffer(cell, 'utf8'));
                    } else if (isInteger(cell)) {
                        tsc.setIntegerValue(cell);
                    } else if (isFloat(cell)) {
                        tsc.setNumericValue(new Buffer(cell.toString()));
                    } else if (isObject(cell)) {
                        tsc.setMapValue(new Buffer(JSON.stringify(cell)));
                    }
                }
                tsr.cells.push(tsc);
            });
            protobuf.rows.push(tsr);
        });
    }

    return protobuf;
};

StoreValue.prototype.onSuccess = function(rpbPutResp) {
    this._callback(null, true);
    return true;
};

var columnTypeSchema = Joi.number().valid(tsdata.validColumnTypes);

var columnSchema = Joi.object().keys({
    name: Joi.string().required(),
    type: columnTypeSchema.required(),
    complexType: Joi.array().items(columnTypeSchema).optional(),
});

var schema = Joi.object().keys({
    table: Joi.string().required(),
    columns: Joi.array().items(columnSchema).optional(),
    rows: Joi.array().items(Joi.array().sparse()).required(),
});

/**
 * A builder for constructing StoreValue instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a StoreValue directly, this builder may be used.
 *
 *     var storeValue = new StoreValue.Builder()
 *          .withTable('table')
 *          .withColumns(columns)
 *          .withRows(rows)
 *          .build();
 *
 * @class StoreValue.Builder
 * @constructor
 */
function Builder() {}

Builder.prototype = {
    /**
     * Set the table.
     * @method withTable
     * @param {String} table the table in Riak
     * @chainable
     */
    withTable : function(table) {
        this.table = table;
        return this;
    },
    /**
     * Set the columns (optional).
     * @method withColumns
     * @param {Object[]} columns the timeseries columns in Riak
     * @chainable
     */
    withColumns : function(columns) {
        this.columns = columns;
        return this;
    },
    /**
     * Set the rows.
     * @method withRows
     * @param {Object[]} rows the timeseries row data
     * @chainable
     */
    withRows : function(rows) {
        this.rows = rows;
        return this;
    },
    /**
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback The allback to be executed when the operation completes.
     * @param {String} callback.err An error message. Will be null if no error.
     * @param {Object} callback.response The response from Riak
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
    /**
     * Construct a StoreValue instance.
     * @method build
     * @return {StoreValue} a StoreValue instance
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new StoreValue(this, cb);
    }
};

module.exports = StoreValue;
module.exports.Builder = Builder;
