var inherits = require('util').inherits;
var Joi = require('joi');
var logger = require('winston');

var ByKeyBase = require('./bykeybase');

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
 * @param {Object} callback.response.columns Timeseries column data
 * @param {Object} callback.response.rows Timeseries row data
 * @extends ByKeyBase
 */
function Get(options, callback) {
    ByKeyBase.call(this, options, 'TsGetReq', 'TsGetResp', callback);
}

inherits(Get, ByKeyBase);

Get.prototype.onSuccess = function(rpbGetResp) {
    // TODO RTS-311 use same value -> TsCell conversion as TS.Store
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
