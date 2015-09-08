var tsdata = require('./data');
var CommandBase = require('../commandbase');

var inherits = require('util').inherits;
var Joi = require('joi');
var logger = require('winston');

var rpb = require('../../../lib/protobuf/riakprotobuf');
var TsInterpolation = rpb.getProtoFor('TsInterpolation');
var TsColumnType = rpb.getProtoFor('TsColumnType');

/**
 * Provides the Query class, its builder, and its response.
 * @module TS
 */

/**
 * Command used to query timeseries data in Riak.
 *
 * As a convenience, a builder class is provided:
 *
 *      var storeValue = new Query.Builder()
 *          .withQuery('select * from timeseries')
 *          .withCallback(callback)
 *          .build();
 *
 * See {{#crossLink "Query.Builder"}}Query.Builder{{/crossLink}}
 *
 * @class Query
 * @constructor
 * @param {Object} options The options for this command.
 * @param {String} options.query The timeseries query for Riak.
 * @param {Function} callback The allback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response Object containing timeseries data.
 * @param {Object} callback.response.columns Timeseries column data
 * @param {Object} callback.response.rows Timeseries row data
 * @extends CommandBase
 */
function Query(options, callback) {
    CommandBase.call(this, 'TsQueryReq', 'TsQueryResp', callback);
    var self = this;
    Joi.validate(options, schema, function(err, options) {
        if (err) {
            throw err;
        }
        self.options = options;
    });
    this.remainingTries = 1;
}

inherits(Query, CommandBase);

Query.prototype.constructPbRequest = function() {
    var protobuf = this.getPbReqBuilder();

    if (this.options.query) {
        var tsi = new TsInterpolation();
        tsi.setBase(new Buffer(this.options.query));
        protobuf.setQuery(tsi);
    }

    return protobuf;
};

function makeSetValueDeserializer(set) {
    return function (rpbval) {
        set.push(JSON.parse(rpbval.toString('utf8')));
    };
}

Query.prototype.onSuccess = function(rpbQueryResp) {
    var response = {
        columns: [],
        rows: [],
    };
    
    if (rpbQueryResp) {
        var rcols = rpbQueryResp.getColumns();
        if (rcols && rcols.length > 0) {
            rcols.forEach(function (rc) {
                response.columns.push({
                    name: rc.getName().toString('utf8'),
                    type: rc.getType()
                });
            });
        }

        var rrows = rpbQueryResp.getRows();
        if (rrows && rrows.length > 0) {
            for (var i = 0; i < rrows.length; i++) {
                var rsprow = [];
                var rcells = rrows[i].getCells();
                for (var j = 0; j < rcells.length; j++) {
                    var rcol = rcols[j];
                    var rcell = rcells[j];
                    var coltype = rcol.getType();
                    switch (coltype) {
                        case TsColumnType.BINARY:
                            // NB: responses will have ByteBuffers for BINARY
                            if (this.options.convertToStrings) {
                                var str = rcell.getBinaryValue().toString('utf8');
                            } else {
                                rsprow.push(rcell.getBinaryValue());
                            }
                            break;
                        case TsColumnType.INTEGER:
                            rsprow.push(rcell.getIntegerValue());
                            break;
                        case TsColumnType.NUMERIC:
                            rsprow.push(rcell.getNumericValue().toString('utf8'));
                            break;
                        case TsColumnType.TIMESTAMP:
                            rsprow.push(rcell.getTimestampValue());
                            break;
                        case TsColumnType.BOOLEAN:
                            rsprow.push(rcell.getBooleanValue());
                            break;
                        case TsColumnType.SET:
                            var rpbset = rcell.getSetValue();
                            if (Array.isArray(rpbset)) {
                                var set = [];
                                var setValueFromJSON = makeSetValueDeserializer(set);
                                rpbset.forEach(setValueFromJSON);
                                rsprow.push(set);
                            } else {
                                rsprow.push(JSON.parse(rpbset.toString('utf8')));
                            }
                            break;
                        case TsColumnType.MAP:
                            rsprow.push(JSON.parse(rcell.getMapValue().toString('utf8')));
                            break;
                        default:
                            throw new Error('unknown column type: ' + coltype);
                    }
                }
                response.rows.push(rsprow);
            }
        }
    }

    this._callback(null, response);
    return true;
};

var schema = Joi.object().keys({
    query: Joi.string().optional(), // TODO RTS-311 why is this optional?
    convertToStrings: Joi.boolean().default(false).optional()
});

/**
 * A builder for constructing Query instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a Query directly, this builder may be used.
 *
 *     var storeValue = new Query.Builder()
 *          .withTable('table')
 *          .withColumns(columns)
 *          .withRows(rows)
 *          .build();
 *
 * @class Query.Builder
 * @constructor
 */
function Builder() {}

Builder.prototype = {
    /**
     * Set the query text.
     * @method withQuery
     * @param {String} query the timeseries query
     * @chainable
     */
    withQuery : function(query) {
        this.query = query;
        return this;
    },
    /**
     * Convert BINARY columns to UTF8 strings.
     * @method withConvertToStrings
     * @param {Boolean} convertToStrings True to convert all BINARY column data to UTF8 strings.
     * @chainable
     */
    withConvertToStrings : function(convertToStrings) {
        this.convertToStrings = convertToStrings;
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
     * Construct a Query instance.
     * @method build
     * @return {Query} a Query instance
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new Query(this, cb);
    }
};

module.exports = Query;
module.exports.Builder = Builder;
