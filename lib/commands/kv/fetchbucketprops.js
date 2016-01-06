'use strict';

var FetchPropsBase = require('./fetchpropsbase');
var inherits = require('util').inherits;
var Joi = require('joi');

/**
 * Provides the FetchBucketProps class, its builder, and its response.
 * @module KV
 */

/**
 * Command used to fetch a bucket's properties from Riak.
 *
 * As a convenience, a builder class is provided:
 *
 *     var fetch = new FetchBucketProps.Builder()
 *         .withBucketType('my_type')
 *         .withBucket('myBucket')
 *         .withCallback(myCallback)
 *         .build();
 *
 * See {{#crossLink "FetchBucketProps.Builder"}}FetchBucketProps.Builder{{/crossLink}}
 * @class FetchBucketProps
 * @constructor
 * @param {Object} options The options for this command
 * @param {String} [options.bucketType=default] The bucket type in riak.
 * @param {String} options.bucket The bucket in riak.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will ne null if no error.
 * @param {Object} callback.response The response from Riak. This is an oject with all the bucket properties.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends FetchPropsBase
 */
function FetchBucketProps(options, callback) {
    FetchPropsBase.call(this, 'RpbGetBucketReq','RpbGetBucketResp', callback);
    this.validateOptions(options, schema);
}

inherits(FetchBucketProps, FetchPropsBase);

FetchBucketProps.prototype.constructPbRequest = function() {
    var protobuf = this.getPbReqBuilder();
    protobuf.setType(new Buffer(this.options.bucketType));
    protobuf.setBucket(new Buffer(this.options.bucket));
    return protobuf;
};

var schema = Joi.object().keys({
   bucket: Joi.string().required(),
   bucketType: Joi.string().default('default')
});

/**
 * A builder for constructing FetchBucketProps instances
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a FetchBucketProps directly, this builder may be used.
 *
 *     var fetch = new FetchBucketProps.Builder()
 *         .withBucketType('my_type')
 *         .withBucket('myBucket')
 *         .withCallback(myCallback)
 *         .build();
 *
 * @class FetchBucketProps.Builder
 * @constructor
 */
function Builder() {
    FetchPropsBase.Builder.call(this);
}

inherits(Builder, FetchPropsBase.Builder);

/**
 * Set the bucket.
 * @method withBucket
 * @param {String} bucket the bucket in Riak
 * @chainable
 */
Builder.prototype.withBucket = function(bucket) {
    this.bucket = bucket;
    return this;
};

/**
    * Construct a FetchBucketProps instance.
    * @method build
    * @return {FetchBucketProps}
    */
Builder.prototype.build = function() {
    var cb = this.callback;
    delete this.callback;
    return new FetchBucketProps(this, cb);
};

module.exports = FetchBucketProps;
module.exports.Builder = Builder;

