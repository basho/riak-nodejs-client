'use strict';

var inherits = require('util').inherits;
var Joi = require('joi');

var StorePropsBase = require('./storepropsbase');

/**
 * Provides the StoreBucketTypeProps class, its builder, and its response.
 * @module KV
 */

/**
 * Command used to set the properties on a bucket type in Riak.
 *
 * As a convenience, a builder class is provided:
 *
 *     var storeProps = new StoreBucketTypeProps.Builder()
 *                  .withBucketType('my-type')
 *                  .withAllowMult(true)
 *                  .build();
 *
 * See {{#crossLink "StoreBucketTypeProps.Builder"}}StoreBucketTypeProps.Builder{{/crossLink}}
 *
 * @class StoreBucketTypeProps
 * @constructor
 * @param {Object} options The properties to store
 * @param {String} [options.bucketType] The bucket type in riak. If not supplied 'default is used'
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Boolean} callback.response the response from Riak. This will be true.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will ne null if no error.
 * @param {Object} callback.response The response from Riak. This is an oject with all the bucket properties.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends StorePropsBase
 */
function StoreBucketTypeProps(options, callback) {
    StorePropsBase.call(this, options, 'RpbSetBucketTypeReq', 'RpbSetBucketResp', callback);
    this.validateOptions(options, schema, { allowUnknown: true });
}

inherits(StoreBucketTypeProps, StorePropsBase);

StoreBucketTypeProps.prototype.constructPbRequest = function() {
    var protobuf = StoreBucketTypeProps.super_.prototype.constructPbRequest.call(this); 
    protobuf.setType(new Buffer(this.options.bucketType));
    return protobuf;
};

/**
 * A builder for constructing StoreBucketTypeProps instances
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a StoreBucketTypeProps directly, this builder may be used.
 *
 *     var storeProps = new StoreBucketTypeProps.Builder()
 *                  .withAllowMult(true)
 *                  .build();
 *
 * @class StoreBucketTypeProps.Builder
 * @constructor
 * @extends StorePropsBase.Builder
 */
function Builder() {
    StorePropsBase.Builder.call(this);
    this.precommit = [];
    this.postcommit = [];
}

inherits(Builder, StorePropsBase.Builder);

var schema = Joi.object().keys({
    bucketType: Joi.string().default('default')
});

/**
 * Construct a StoreBucketTypeProps instance.
 * @method build
 * @return {StoreBucketTypeProps}
 */
Builder.prototype.build = function() {
    var cb = this.callback;
    delete this.callback;
    return new StoreBucketTypeProps(this, cb);
};

module.exports = StoreBucketTypeProps;
module.exports.Builder = Builder;
