'use strict';

var util = require('util');
var Joi = require('joi');
var logger = require('winston');

var ByteBuffer = require('bytebuffer');

var CommandBase = require('../commandbase');
var tsdata = require('./data');
var tsutils = require('./utils');
var utils = require('../../utils');

var rpb = require('../../protobuf/riakprotobuf');
var TsColumnType = rpb.getProtoFor('TsColumnType');
var TsColumnDescription = rpb.getProtoFor('TsColumnDescription');
var TsRow = rpb.getProtoFor('TsRow');
var TsCell = rpb.getProtoFor('TsCell');

/**
 * Provides the Store class, its builder, and its response.
 * @module TS
 */

/**
 * Command used to store timeseries data in Riak.
 *
 * As a convenience, a builder class is provided:
 *
 *      var storeValue = new Store.Builder()
 *          .withTable('table')
 *          .withColumns(cols)
 *          .withRows(rows)
 *          .build();
 *
 * See {{#crossLink "Store.Builder"}}Store.Builder{{/crossLink}}
 *
 * @class Store
 * @constructor
 * @param {Object} options The options for this command.
 * @param {String} options.table The timeseries table in Riak.
 * @param {Object[]} options.columns The timeseries columns in Riak.
 * @param {Object[]} options.rows The timeseries rows in Riak.
 * @param {Function} callback The allback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response Will be either true or false.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function Store(options, callback) {
    CommandBase.call(this, 'TsPutReq', 'TsPutResp', callback);
    this.validateOptions(options, schema);
}

util.inherits(Store, CommandBase);

Store.prototype.constructPbRequest = function() {
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
                if (!utils.isNullOrUndefined(cell)) {
                    switch (ct) {
                        case TsColumnType.VARCHAR:
                        case TsColumnType.BLOB:
                            if (Buffer.isBuffer(cell)) {
                                tsc.setVarcharValue(cell);
                            } else if (ByteBuffer.isByteBuffer(cell)) {
                                tsc.setVarcharValue(cell.toBuffer());
                            } else {
                                tsc.setVarcharValue(new Buffer(cell));
                            }
                            break;
                        case TsColumnType.SINT64:
                            // NB: must be convertible to Long to serialize as sint64
                            tsc.setSint64Value(cell);
                            break;
                        case TsColumnType.DOUBLE:
                            tsc.setDoubleValue(cell);
                            break;
                        case TsColumnType.TIMESTAMP:
                            // NB: must be convertible to Long to serialize as sint64
                            var val = tsutils.maybeConvertToLong(cell);
                            tsc.setTimestampValue(val);
                            break;
                        case TsColumnType.BOOLEAN:
                            tsc.setBooleanValue(cell);
                            break;
                        default:
                            var msg = util.format("could not serialize: %s, column type: %d",
                                            JSON.stringify(cell), ct);
                            throw new Error(msg);
                    }
                }
                tsr.cells.push(tsc);
            }
            protobuf.rows.push(tsr);
        }
    } else {
        this.options.rows.forEach(function (row) {
            var cells = tsutils.convertToTsCells(row);
            var tsr = new TsRow();
            Array.prototype.push.apply(tsr.cells, cells);
            protobuf.rows.push(tsr);
        });
    }

    return protobuf;
};

Store.prototype.onSuccess = function(rpbPutResp) {
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
 * A builder for constructing Store instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a Store directly, this builder may be used.
 *
 *     var storeValue = new Store.Builder()
 *          .withTable('table')
 *          .withColumns(columns)
 *          .withRows(rows)
 *          .build();
 *
 * @class Store.Builder
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
     * Construct a Store instance.
     * @method build
     * @return {Store} a Store instance
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new Store(this, cb);
    }
};

module.exports = Store;
module.exports.Builder = Builder;
