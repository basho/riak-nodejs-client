'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');

/**
 * Provides the FetchCounter class, its builder, and its response.
 * @module CRDT
 */

/**
 *  Command used to fetch a counter value from Riak.
 *
 *  As a convenience, a builder class is provided:
 *
 *      var fetch = new FetchCounter.Builder()
 *                      .withBucketType('myBucketType')
 *                      .withBucket('myBucket')
 *                      .withKey('myKey')
 *                      .withCallback(myCallback)
 *                      .build();
 *
 * See {{#crossLink "FetchCounter.Builder"}}FetchCounter.Builder{{/crossLink}}
 *
 * @class FetchCounter
 * @constructor
 * @param {Object} options The options to use for this command.
 * @param {String} options.bucketType The bucket type in Riak.
 * @param {String} options.bucket The bucket in Riak.
 * @param {String} options.key The key for the counter you want to fetch.
 * @param {Number} [options.timeout] Set a timeout in Riak for this operation.
 * @param {Number} [options.r] The R value to use for this fetch.
 * @param {Number} [options.pr] The PR value to use for this fetch.
 * @param {Boolean} [options.notFoundOk] If true a vnode returning notfound for a key increments the R tally.
 * @param {Boolean} [options.useBasicQuorum] Controls whether a read request should return early in some fail cases.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response The response from Riak.
 * @param {Number} callback.response.counterValue The counter value in Riak.
 * @param {Boolean} callback.response.isNotFound True if there was no counter in Riak.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function FetchCounter(options, callback) {
    CommandBase.call(this, 'DtFetchReq', 'DtFetchResp', callback);
    this.validateOptions(options, schema);
}

inherits(FetchCounter, CommandBase);

FetchCounter.prototype.constructPbRequest = function() {

    var protobuf = this.getPbReqBuilder();

    protobuf.setBucket(new Buffer(this.options.bucket));
    protobuf.setType(new Buffer(this.options.bucketType));
    protobuf.setKey(new Buffer(this.options.key));
    protobuf.setR(this.options.r);
    protobuf.setPr(this.options.pr);
    protobuf.setBasicQuorum(this.options.basicQuorum);
    protobuf.setNotfoundOk(this.options.notFoundOk);
    protobuf.setTimeout(this.options.timeout);

    return protobuf;

};

FetchCounter.prototype.onSuccess = function(dtFetchResp) {

    var value = null;
    var isNotFound = false;
    if (dtFetchResp.getValue()) {
        // sint64 is weird. You either get the zigzag encoding, or
        // you can get the actual number via the bytebuffer.
        value = dtFetchResp.value.counter_value.toNumber();

    } else {
        isNotFound = true;
    }
    // TODO 2.0 - remove notFound
    this._callback(null, { counterValue: value, isNotFound: isNotFound, notFound: isNotFound });

    return true;
};

var schema = Joi.object().keys({
   bucket: Joi.string().required(),
   bucketType: Joi.string().required(),
   key: Joi.binary().required(),
   r: Joi.number().default(null).optional(),
   pr: Joi.number().default(null).optional(),
   notFoundOk: Joi.boolean().default(null).optional(),
   basicQuorum: Joi.boolean().default(null).optional(),
   timeout: Joi.number().default(null).optional()
});

/**
 * A builder for constructing FetchCounter instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a FetchCounter directly, this builder may be used.
 *
 *     var fetch = new FetchCounter.Builder()
 *                      .withBucketType('myBucketType')
 *                      .withBucket('myBucket')
 *                      .withKey('myKey')
 *                      .withCallback(myCallback)
 *                      .build();
 *
 * @class FetchCounter.Builder
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
     * Set the R value.
     * If not asSet the bucket default is used.
     * @method withR
     * @param {Number} r the R value.
     * @chainable
     */
    withR : function(r) {
        this.r = r;
        return this;
    },
    /**
    * Set the PR value.
    * If not asSet the bucket default is used.
    * @method withPr
    * @param {Number} pr the PR value.
    * @chainable
    */
    withPr : function(pr) {
        this.pr = pr;
        return this;
    },/**
    * Set the not_found_ok value.
    * If true a vnode returning notfound for a key increments the r tally.
    * False is higher consistency, true is higher availability.
    * If not asSet the bucket default is used.
    * @method withNotFoundOk
    * @param {Boolean} notFoundOk the not_found_ok value.
    * @chainable
    */
    withNotFoundOk : function(notFoundOk) {
        this.notFoundOk = notFoundOk;
        return this;
    },
    /**
    * Set the basic_quorum value.
    * The parameter controls whether a read request should return early in
    * some fail cases.
    * E.g. If a quorum of nodes has already
    * returned notfound/error, don't wait around for the rest.
    * @method withBasicQuorum
    * @param {Boolean} useBasicQuorum the basic_quorum value.
    * @chainable
    */
    withBasicQuorum : function(useBasicQuorum) {
        this.basicQuorum = useBasicQuorum;
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
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback The callback to execute
     * @param {String} callback.err An error message. Will be null if no error.
     * @param {Object} callback.response The response from Riak
     * @param {Number} callback.response.counterValue The counter value in Riak.
     * @param {Boolean} callback.response.isNotFound True if there was no counter in Riak.
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
    /**
     * Construct a FetchCounter instance.
     * @method build
     * @return {FetchCounter}
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new FetchCounter(this, cb);
    }

};

module.exports = FetchCounter;
module.exports.Builder = Builder;
