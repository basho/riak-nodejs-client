'use strict';

var inherits = require('util').inherits;
var Joi = require('joi');
var logger = require('winston');

var ByKeyBase = require('./bykeybase');

/**
 * Provides the Delete class, its builder, and its response.
 * @module TS
 */

/**
 * Command used to delete timeseries data from Riak.
 *
 * As a convenience, a builder class is provided:
 *
 *      var get = new Delete.Builder()
 *          .withKey(key)
 *          .withCallback(callback)
 *          .build();
 *
 * See {{#crossLink "Delete.Builder"}}Delete.Builder{{/crossLink}}
 *
 * @class Delete
 * @constructor
 * @param {Function} callback The allback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response true or false to indicate success / failure.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends ByKeyBase
 */
function Delete(options, callback) {
    ByKeyBase.call(this, options, 'TsDelReq', 'TsDelResp', callback);
}

inherits(Delete, ByKeyBase);

Delete.prototype.onSuccess = function(rpbDelResp) {
    this._callback(null, true);
    return true;
};

/**
 * A builder for constructing Delete instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a Delete directly, this builder may be used.
 *
 *     var get = new Delete.Builder()
 *          .withKey(key)
 *          .build();
 *
 * @class Delete.Builder
 * @constructor
 * @extends ByKeyBase.Builder
 */
function Builder() {
    ByKeyBase.Builder.call(this);
}

inherits(Builder, ByKeyBase.Builder);

/**
 * Construct a Delete instance.
 * @method build
 * @return {Delete} a Delete instance
 */
Builder.prototype.build = function() {
    var cb = this.callback;
    delete this.callback;
    return new Delete(this, cb);
};

module.exports = Delete;
module.exports.Builder = Builder;
