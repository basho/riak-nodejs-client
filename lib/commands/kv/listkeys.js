'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');

/**
 * Provides the ListKeys class, its builder, and its response.
 * @module KV
 */

/**
 * Command used to list keys in a bucket.
 *
 * Note that this is a __very__ expensive operation and not recommended for production use.
 *
 * As a convenience, a builder class is provided;
 *
 *     var listKeys = new ListKeys.Builder()
 *                  .withBucketType('myBucketType')
 *                  .withBucket('myBucket')
 *                  .withCallback(myCallback)
 *                  .build();
 *
 * See {{#crossLink "ListKeys.Builder"}}ListKeys.Builder{{/crossLink}}
 * @class ListKeys
 * @constructor
 * @param {Object} options The options for this command
 * @param {String} [options.bucketType=default] The bucket type in riak. If not supplied 'default' is used.
 * @param {String} options.bucket The bucket in riak.
 * @param {Boolean} [options.stream=true] Whether to stream or accumulate the result before calling callback.
 * @param {Number} [options.timeout] Set a timeout for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response the keys returned from Riak.
 * @param {Boolean} callback.response.done True if you have received all the keys.
 * @param {String} callback.response.bucketType The bucketType the keys are from.
 * @param {String} callback.response.bucket The bucket the keys are from.
 * @param {String[]} callback.response.keys The array of keys.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function ListKeys(options, callback) {
    CommandBase.call(this, 'RpbListKeysReq', 'RpbListKeysResp', callback);
    this.validateOptions(options, schema);
    if (!this.options.stream) {
        this.keys = [];
    }
}

inherits(ListKeys, CommandBase);

ListKeys.prototype.constructPbRequest = function() {
    var protobuf = this.getPbReqBuilder();

    protobuf.setBucket(new Buffer(this.options.bucket));
    protobuf.setType(new Buffer(this.options.bucketType));
    protobuf.setTimeout(this.options.timeout);

    return protobuf;
};

ListKeys.prototype.onSuccess = function(rpbListKeysResp) {

    var keysToSend = new Array(rpbListKeysResp.keys.length);
    if (rpbListKeysResp.keys.length) {
        for (var i = 0; i < rpbListKeysResp.keys.length; i++) {
            keysToSend[i] = rpbListKeysResp.keys[i].toString('utf8');
        }
    }

    if (this.options.stream) {
        this._callback(null, { bucketType: this.options.bucketType,
            bucket: this.options.bucket,
            keys: keysToSend, done: rpbListKeysResp.done});
    } else {
        Array.prototype.push.apply(this.keys, keysToSend);
        if (rpbListKeysResp.done) {
            this._callback(null, {  bucketType: this.options.bucketType,
                bucket: this.options.bucket, keys: this.keys,
                done: rpbListKeysResp.done });
        }
    }

    return rpbListKeysResp.done;

};

var schema = Joi.object().keys({
   bucket: Joi.string().required(),
   bucketType: Joi.string().default('default'),
   stream : Joi.boolean().default(true),
   timeout: Joi.number().default(null)
});

/**
 * A builder for constructing ListKeys instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a ListKeys directly, this builder may be used.
 *
 *     var listKeys = new ListKeys.Builder()
 *                  .withBucketType('myBucketType')
 *                  .withBucket('myBucket')
 *                  .withCallback(myCallback)
 *                  .build();
 *
 * @class ListKeys.Builder
 * @constructor
 */
function Builder() {}

Builder.prototype = {

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
     * Stream the results.
     * Setting this to true will cause you callback to be called as the results
     * are returned from Riak. Set to false the result set will be buffered and
     * delevered via a single call to your callback. Note that on large result sets
     * this is very memory intensive.
     * @method withStreaming
     * @param {Boolean} [stream=true] Set whether or not to stream the results
     * @chainable
     */
    withStreaming : function(stream) {
        this.stream = stream;
        return this;
    },
    /**
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback The callback to be executed when the operation completes.
     * @param {String} callback.err An error message. Will be null if no error.
     * @param {Object} callback.response the keys returned from Riak.
     * @param {Boolean} callback.response.done True if you have received all the keys.
     * @param {String} callback.response.bucketType The bucketType the keys are from.
     * @param {String} callback.response.bucket The bucket the keys are from.
     * @param {String[]} callback.response.keys The array of keys.
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
    /**
    * Set a timeout for this operation.
    * @method withTimeout
    * @param {Number} timeout a timeout in milliseconds.
    * @chainable
    */
    withTimeout : function(timeout) {
        this.timeout = timeout;
        return this;
    },
    /**
     * Construct a ListKeys instance.
     * @method build
     * @return {ListKeys}
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new ListKeys(this, cb);
    }

};

module.exports = ListKeys;
module.exports.Builder = Builder;
