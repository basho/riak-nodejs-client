'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var logger = require('winston');
var Joi = require('joi');

/**
 * Provides the ResetBucketProps class and its builder.
 * @module KV
 */

/**
 * Command used to reset a bucket's properties in Riak 
 * to the default values
 *
 * As a convenience, a builder class is provided:
 *
 *     var fetch = new ResetBucketProps.Builder()
 *         .withBucketType('myBucketType')
 *         .withBucket('myBucket')
 *         .withCallback(myCallback)
 *         .build();
 *
 * See {{#crossLink "ResetBucketProps.Builder"}}ResetBucketProps.Builder{{/crossLink}}
 *
 * @class ResetBucketProps
 * @constructor
 * @param {Object} options The options for this command.
 * @param {String} [options.bucketType=default] The bucket type in riak. If not supplied 'default' is used.
 * @param {String} options.bucket The bucket in riak.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response the response from Riak.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */

function ResetBucketProps(options, callback) {
    CommandBase.call(this, 'RpbResetBucketReq', 'RpbResetBucketResp', callback);
    this.validateOptions(options, schema);
}

inherits(ResetBucketProps, CommandBase);

ResetBucketProps.prototype.constructPbRequest = function() {
    var protobuf = this.getPbReqBuilder();
    protobuf.setType(new Buffer(this.options.bucketType));
    protobuf.setBucket(new Buffer(this.options.bucket));
    return protobuf;
};

ResetBucketProps.prototype.onSuccess = function(rpbGetResp) {
    this._callback(null, true);
    return true;
};

var schema = Joi.object().keys({
   bucketType: Joi.string().default('default'),
   bucket: Joi.string().required()
});

/**
 * A builder for constructing ResetBucketProps instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a ResetBucketProps directly, this builder may be used.
 *
 *     var fetchValue = new ResetBucketProps.Builder()
 *          .withBucketType('myBucketType')
 *          .withBucket('myBucket')
 *          .build();
 *
 * @class ResetBucketProps.Builder
 * @constructor
 */
function Builder() {}

Builder.prototype = {
    /**
     * Set the bucket type.
     * If not supplied, 'default' is used.
     * @method withBucketType
     * @param {String} bucketType the bucket type in riak
     * @chainable
     */
    withBucketType : function(bucketType) {
        this.bucketType = bucketType;
        return this;
    },
    /**
     * Set the bucket.
     * @method withBucket
     * @param {String} bucket the bucket in Riak
     * @chainable
     */
    withBucket : function(bucket) {
        this.bucket = bucket;
        return this;
    },
    /**
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback The callback to be executed when the operation completes.
     * @param {String} callback.err An error message. Will ne null if no error.
     * @param {Object} callback.response the response from Riak.
     * @param {Object[]} callback.response.preflist An array of one or more preflist items.
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
    /**
     * Construct a ResetBucketProps command.
     * @method build
     * @return {ResetBucketProps}
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new ResetBucketProps(this, cb);
    }
};

module.exports = ResetBucketProps;
module.exports.Builder = Builder;
