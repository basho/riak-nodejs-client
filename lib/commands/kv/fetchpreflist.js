'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var logger = require('winston');
var Joi = require('joi');

/**
 * Provides the FetchPreflist class and its builder.
 * @module KV
 */

/**
 * Command used to fetch an object's preflist from Riak.
 *
 * As a convenience, a builder class is provided:
 *
 *     var fetch = new FetchPreflist.Builder()
 *         .withBucketType('myBucketType')
 *         .withBucket('myBucket')
 *         .withKey('myKey')
 *         .withCallback(myCallback)
 *         .build();
 *
 * See {{#crossLink "FetchPreflist.Builder"}}FetchPreflist.Builder{{/crossLink}}
 *
 * @class FetchPreflist
 * @constructor
 * @param {Object} options The options for this command.
 * @param {String} [options.bucketType=default] The bucket type in riak. If not supplied 'default' is used.
 * @param {String} options.bucket The bucket in riak.
 * @param {String} options.key The key for the object you want to fetch.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response the response from Riak.
 * @param {Object[]} callback.response.preflist An array of one or more preflist entries.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */

function FetchPreflist(options, callback) {
    CommandBase.call(this, 'RpbGetBucketKeyPreflistReq', 'RpbGetBucketKeyPreflistResp', callback);
    this.validateOptions(options, schema);
}

inherits(FetchPreflist, CommandBase);

FetchPreflist.prototype.constructPbRequest = function() {

    var protobuf = this.getPbReqBuilder();

    protobuf.setType(new Buffer(this.options.bucketType));
    protobuf.setBucket(new Buffer(this.options.bucket));
    protobuf.setKey(new Buffer(this.options.key));

    return protobuf;

};

FetchPreflist.prototype.onSuccess = function(rpbGetResp) {

    var response = { preflist: [] };

    if (rpbGetResp) {
        var pbPreflistArray = rpbGetResp.getPreflist();
        if (pbPreflistArray.length !== 0) {
            var preflist = new Array(pbPreflistArray.length);
            for (var i = 0; i < pbPreflistArray.length; ++i) {
                var pi = pbPreflistArray[i];
                preflist[i] = {
                    partition: pi.partition.toNumber(),
                    node: pi.node.toString('utf8'),
                    primary: pi.primary
                };
            }
            response = { preflist: preflist };
        }
    }

    this._callback(null, response);

    return true;
};

var schema = Joi.object().keys({
   bucketType: Joi.string().default('default'),
   bucket: Joi.string().required(),
   key: Joi.binary().required()
});

/**
 * A builder for constructing FetchPreflist instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a FetchPreflist directly, this builder may be used.
 *
 *     var fetchValue = new FetchPreflist.Builder()
 *          .withBucketType('myBucketType')
 *          .withBucket('myBucket')
 *          .withKey('myKey')
 *          .build();
 *
 * @class FetchPreflist.Builder
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
     * Set the key.
     * @method withKey
     * @param {String} key the key in riak.
     * @chainable
     */
    withKey : function(key) {
        this.key = key;
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
     * Construct a FetchPreflist command.
     * @method build
     * @return {FetchPreflist}
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new FetchPreflist(this, cb);
    }
};

module.exports = FetchPreflist;
module.exports.Builder = Builder;
