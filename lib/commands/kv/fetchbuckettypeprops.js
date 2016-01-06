'use strict';

var FetchPropsBase = require('./fetchpropsbase');
var inherits = require('util').inherits;
var Joi = require('joi');

/**
 * Provides the FetchBucketTypeProps class, its builder, and its response.
 * @module KV
 */

/**
 * Command used to fetch a bucket's properties from Riak.
 *
 * As a convenience, a builder class is provided:
 *
 *     var fetch = new FetchBucketTypeProps.Builder()
 *         .withBucketType('my_type')
 *         .withCallback(myCallback)
 *         .build();
 *
 * See {{#crossLink "FetchBucketTypeProps.Builder"}}FetchBucketTypeProps.Builder{{/crossLink}}
 * @class FetchBucketTypeProps
 * @constructor
 * @param {Object} options The options for this command
 * @param {String} [options.bucketType=default] The bucket type in riak.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will ne null if no error.
 * @param {Object} callback.response The response from Riak. This is an oject with all the bucket type properties.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends FetchPropsBase
 */
function FetchBucketTypeProps(options, callback) {
    FetchPropsBase.call(this, 'RpbGetBucketTypeReq','RpbGetBucketResp', callback);
    this.validateOptions(options, schema);
}

inherits(FetchBucketTypeProps, FetchPropsBase);

FetchBucketTypeProps.prototype.constructPbRequest = function() {
    var protobuf = this.getPbReqBuilder();
    protobuf.setType(new Buffer(this.options.bucketType));
    return protobuf;
};

var schema = Joi.object().keys({
   bucketType: Joi.string().default('default')
});

/**
 * A builder for constructing FetchBucketTypeProps instances
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a FetchBucketTypeProps directly, this builder may be used.
 *
 *     var fetch = new FetchBucketTypeProps.Builder()
 *         .withBucketType('my_type')
 *         .withCallback(myCallback)
 *         .build();
 *
 * @class FetchBucketTypeProps.Builder
 * @constructor
 */
function Builder() {
    FetchPropsBase.Builder.call(this);
}

inherits(Builder, FetchPropsBase.Builder);

/**
 * Construct a FetchBucketTypeProps instance.
 * @method build
 * @return {FetchBucketTypeProps}
 */
Builder.prototype.build = function() {
    var cb = this.callback;
    delete this.callback;
    return new FetchBucketTypeProps(this, cb);
};

module.exports = FetchBucketTypeProps;
module.exports.Builder = Builder;

