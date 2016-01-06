'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');
var DtOp = require('../../protobuf/riakprotobuf').getProtoFor('DtOp');
var CounterOp = require('../../protobuf/riakprotobuf').getProtoFor('CounterOp');

/**
 * Provides the UpdateCounter class, its builder, and its response.
 * @module CRDT
 */

/**
 * Command used to update a Counter in Riak
 *
 * As a convenience, a builder class is provided:
 *
 *     var update = new UpdateCounter.Builder()
 *               .withBucketType('counters')
 *               .withBucket('myBucket')
 *               .withKey('counter_1')
 *               .withIncrement(100)
 *               .withCallback(callback)
 *               .build();
 *
 * See {{#crossLink "UpdateCounter.Builder"}}UpdateCounter.Builder{{/crossLink}}
 *
 * @module CRDT
 * @class UpdateCounter
 * @constructor
 * @param {Object} options The options to use for this command.
 * @param {String} options.bucketType The bucket type in riak.
 * @param {String} options.bucket The bucket in riak.
 * @param {String} [options.key] The key for the counter you want to store. If not provided Riak will generate and return one.
 * @param {Number} [options.w] The W value to use.
 * @param {Number} [options.dw] The DW value to use.
 * @param {Number} [options.pw] The PW value to use.
 * @param {Boolean} [options.returnBody=true] Return the counter.
 * @param {Number} [options.timeout] Set a timeout for this command.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response The response from Riak. Will ne null if returnBody not set.
 * @param {String} callback.response.generatedKey If no key was supplied, Riak will generate and return one here.
 * @param {Number} callback.response.counterValue The value of the counter in Riak.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function UpdateCounter(options, callback) {
    CommandBase.call(this, 'DtUpdateReq', 'DtUpdateResp', callback);
    this.validateOptions(options, schema);
}

inherits(UpdateCounter, CommandBase);

UpdateCounter.prototype.constructPbRequest = function() {

    var protobuf = this.getPbReqBuilder();

    protobuf.setBucket(new Buffer(this.options.bucket));
    protobuf.setType(new Buffer(this.options.bucketType));

    // key can be null to have Riak generate it.
    if (this.options.key) {
        protobuf.setKey(new Buffer(this.options.key));
    }

    protobuf.setTimeout(this.options.timeout);
    protobuf.setW(this.options.w);
    protobuf.setPw(this.options.pw);
    protobuf.setDw(this.options.dw);
    protobuf.setReturnBody(this.options.returnBody);

    var dtOp = new DtOp();
    var counterOp = new CounterOp();
    counterOp.increment = this.options.increment;
    dtOp.counter_op = counterOp;
    protobuf.setOp(dtOp);

    return protobuf;

};

UpdateCounter.prototype.onSuccess = function(dtUpdateResp) {

    // dtUpdateResp will be null if returnBody wasn't specified
    if (dtUpdateResp) {
        var key = null;
        if (dtUpdateResp.key) {
            key = dtUpdateResp.key.toString('utf8');
        }

        // sint64 is weird. You either get the zigzag encoding, or
        // you can get the actual number via the bytebuffer.
        var value = dtUpdateResp.counter_value.toNumber();
        var response = { generatedKey: key, counterValue: value };

        this._callback(null, response);
    } else {
        this._callback(null, null);
    }

};

var schema = Joi.object().keys({
    bucket: Joi.string().required(),
    bucketType: Joi.string().required(),
    key: Joi.binary().default(null).optional(),
    increment: Joi.number().required(),
    w: Joi.number().default(null).optional(),
    dw: Joi.number().default(null).optional(),
    pw: Joi.number().default(null).optional(),
    returnBody: Joi.boolean().default(true).optional(),
    timeout: Joi.number().default(null).optional()
});

/**
 * A builder for constructing UpdateCounter instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a UpdateCounter directly, this builder may be used.
 *
 *     var update = new UpdateCounter.Builder()
 *               .withBucketType('counters')
 *               .withBucket('myBucket')
 *               .withKey('counter_1')
 *               .withIncrement(100)
 *               .withCallback(callback)
 *               .build();
 *
 * @class UpdateCounter.Builder
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
     *
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
     * If not set Riak will generate one.
     * @method withKey
     * @param {String} key the key in riak.
     * @chainable
     */
    withKey : function(key) {
        this.key = key;
        return this;
    },
    /**
     * Set the increment to apply to this counter.
     * This may be negative as well as positive.
     * @method withIncrement
     * @param {Number} increment the amount to increment (negative to decrement)
     * @chainable
     */
    withIncrement : function(increment) {
        this.increment = increment;
        return this;
    },
    /**
    * Set the W value.
    * How many replicas to write to before returning a successful response.
    * If not set the bucket default is used.
    * @method withW
    * @param {number} w the W value.
    * @chainable
    */
    withW : function(w) {
        this.w = w ;
        return this;
    },
    /**
     * Set the DW value.
     * How many replicas to commit to durable storage before returning a successful response.
     * If not set the bucket default is used.
     * @method withDw
     * @param {number} dw the DW value.
     * @chainable
     */
    withDw : function(dw) {
        this.dw = dw;
        return this;
    },
    /**
     * Set the PW value.
     * How many primary nodes must be up when the write is attempted.
     * If not set the bucket default is used.
     * @method withPw
     * @param {number} pw the PW value.
     * @chainable
     */
    withPw : function(pw) {
        this.pw = pw;
        return this;
    },
    /**
    * Return the counter after updating.
    * @method withReturnBody
    * @param {boolean} returnBody true to return the counter.
    * @chainable
    */
    withReturnBody: function(returnBody) {
        this.returnBody = returnBody;
        return this;
    },
    /**
    * Set a timeout for this operation.
    * @method withTimeout
    * @param {number} timeout a timeout in milliseconds.
    * @chainable
    */
    withTimeout : function(timeout) {
        this.timeout = timeout;
        return this;
    },
    /**
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback The callback to be executed when the operation completes.
     * @param {String} callback.err An error message. Will be null if no error.
     * @param {Object} callback.response The response from Riak. Will ne null if returnBody not set.
     * @param {String} callback.response.generatedKey If no key was supplied, Riak will generate and return one here.
     * @param {Number} callback.response.counterValue The value of the counter in Riak.
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
    /**
     * Construct a UpdateCounter instance.
     * @method build
     * @return {UpdateCounter} a UpdateCounter instance
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new UpdateCounter(this, cb);
    }
};

module.exports = UpdateCounter;
module.exports.Builder = Builder;

