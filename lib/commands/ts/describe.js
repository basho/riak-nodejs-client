'use strict';

var inherits = require('util').inherits;
var Joi = require('joi');
var logger = require('winston');

var Query = require('./query');

/**
 * Provides the Describe class, its builder, and its response.
 * @module TS
 */

/**
 * Command used to get a timeseries table's description.
 *
 * As a convenience, a builder class is provided:
 *
 *      var storeValue = new Describe.Builder()
 *          .withTable('GeoCheckin')
 *          .withCallback(callback)
 *          .build();
 *
 * See {{#crossLink "Describe.Builder"}}Describe.Builder{{/crossLink}}
 *
 * @class Describe
 * @constructor
 * @param {Object} options The options for this command.
 * @param {String} options.table The timeseries table in Riak.
 * @param {Function} callback The allback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response Object containing timeseries table metadata.
 * @param {Object} callback.response.columns Timeseries table metadata columns.
 * @param {Object} callback.response.rows Timeseries table metadata rows.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends Query
 */
function Describe(options, callback) {
    var self = this;
    Joi.validate(options, schema, function(err, options) {
        if (err) {
            throw err;
        }
        self.options = options;
    });

    var queryOptions = {
        query : 'DESCRIBE ' + self.options.table
    };

    Query.call(this, queryOptions, callback);
}

inherits(Describe, Query);

var schema = Joi.object().keys({
    table: Joi.string().required()
});

/**
 * A builder for constructing Describe instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a Describe directly, this builder may be used.
 *
 *     var storeValue = new Describe.Builder()
 *          .withTable('table')
 *          .build();
 *
 * @class Describe.Builder
 * @constructor
 */
function Builder() {}

Builder.prototype = {
    /**
     * Set the timeseries table.
     * @method withDescribe
     * @param {String} table the timeseries table
     * @chainable
     */
    withTable : function(table) {
        this.table = table;
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
     * Construct a Describe instance.
     * @method build
     * @return {Describe} a Describe instance
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new Describe(this, cb);
    }
};

module.exports = Describe;
module.exports.Builder = Builder;
