'use strict';

var inherits = require('util').inherits;
var Joi = require('joi');
var logger = require('winston');

var CommandBase = require('../commandbase');
var tsdata = require('./data');
var tsutils = require('./utils');

var rpb = require('../../../lib/protobuf/riakprotobuf');
var TsInterpolation = rpb.getProtoFor('TsInterpolation');

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
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function Query(options, callback) {
    CommandBase.call(this, 'TsQueryReq', 'TsQueryResp', callback);
    this.validateOptions(options, schema);
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

Query.prototype.onSuccess = function(rpbQueryResp) {
    var response = tsutils.convertToResponse(rpbQueryResp);
    this._callback(null, response);
    return true;
};

var schema = Joi.object().keys({
    query: Joi.string().optional() // TODO RTS-311 why is this optional?
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
