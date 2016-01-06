'use strict';

var inherits = require('util').inherits;
var Joi = require('joi');
var logger = require('winston');

var CommandBase = require('../commandbase');
var tsdata = require('./data');
var tsutils = require('./utils');

/**
 * Base class for Get and Delete classes.
 * @module TS
 */

/**
 * @class ByKeyBase
 * @constructor
 * @param {Object} options The options for this command.
 * @param {String} options.table The timeseries table from which retrieve a key from Riak.
 * @param {Object[]} options.key The timeseries composite key to retrieve from Riak.
 * @param {Function} callback The allback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response Object containing timeseries data.
 * @param {Object} callback.response.columns Timeseries column data
 * @param {Object} callback.response.rows Timeseries row data
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function ByKeyBase(options, pbRequestName, pbResponseName, callback) {
    CommandBase.call(this, pbRequestName, pbResponseName, callback);
    this.validateOptions(options, schema);
}

inherits(ByKeyBase, CommandBase);

ByKeyBase.prototype.constructPbRequest = function() {
    var protobuf = this.getPbReqBuilder();
    protobuf.setTable(new Buffer(this.options.table));
    var cells = tsutils.convertToTsCells(this.options.key);
    Array.prototype.push.apply(protobuf.key, cells);
    return protobuf;
};

var schema = Joi.object().keys({
    table: Joi.string().required(),
    key: Joi.array().required()
});

/**
 * A builder for constructing Get / Delete command instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a ByKeyBase directly, this builder may be used.
 *
 *     var get = new Get.Builder()
 *          .withKey(key)
 *          .build();
 *
 * @class ByKeyBase.Builder
 * @constructor
 */
function Builder() {}

/**
 * Set the table.
 * @method withTable
 * @param {String} table the table in Riak
 * @chainable
 */
Builder.prototype.withTable = function(table) {
    this.table = table;
    return this;
};

/**
 * Set the key
 * @method withKey
 * @param {Object[]} key the timeseries key value
 * @chainable
 */
Builder.prototype.withKey = function(key) {
    this.key = key;
    return this;
};

/**
 * Set the callback to be executed when the operation completes.
 * @method withCallback
 * @param {Function} callback The allback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response The response from Riak
 * @chainable
 */
Builder.prototype.withCallback = function(callback) {
    this.callback = callback;
    return this;
};

module.exports = ByKeyBase;
module.exports.Builder = Builder;
