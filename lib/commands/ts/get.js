'use strict';

var inherits = require('util').inherits;
var Joi = require('joi');
var logger = require('winston');

var ByKeyBase = require('./bykeybase');
var tsutils = require('./utils');

/**
 * Provides the Get class, its builder, and its response.
 * @module TS
 */

/**
 * Command used to get timeseries data in Riak.
 *
 * As a convenience, a builder class is provided:
 *
 *      var get = new Get.Builder()
 *          .withKey(key)
 *          .withCallback(callback)
 *          .build();
 *
 * See {{#crossLink "Get.Builder"}}Get.Builder{{/crossLink}}
 *
 * @class Get
 * @constructor
 * @param {Function} callback The allback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response.columns Timeseries column data
 * @param {Object} callback.response.rows Timeseries row data
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends ByKeyBase
 */
function Get(options, callback) {
    ByKeyBase.call(this, options, 'TsGetReq', 'TsGetResp', callback);
}

inherits(Get, ByKeyBase);

Get.prototype.onSuccess = function(rpbGetResp) {
    // NB: rpbGetResp is TsGetResp, same as TsQueryResp
    // columns, rows
    var response = tsutils.convertToResponse(rpbGetResp);
    this._callback(null, response);
    return true;
};

/**
 * A builder for constructing Get instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a Get directly, this builder may be used.
 *
 *     var get = new Get.Builder()
 *          .withKey(key)
 *          .build();
 *
 * @class Get.Builder
 * @constructor
 * @extends ByKeyBase.Builder
 */
function Builder() {
    ByKeyBase.Builder.call(this);
}

inherits(Builder, ByKeyBase.Builder);

/**
 * Construct a Get instance.
 * @method build
 * @return {Get} a Get instance
 */
Builder.prototype.build = function() {
    var cb = this.callback;
    delete this.callback;
    return new Get(this, cb);
};

module.exports = Get;
module.exports.Builder = Builder;
