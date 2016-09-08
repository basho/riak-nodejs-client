'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');
var ByteBuffer = require('bytebuffer');

var utils = require('../../utils');
var rpb = require('../../protobuf/riakprotobuf');
var DtOp = rpb.getProtoFor('DtOp');
var SetOp = rpb.getProtoFor('SetOp');

/**
 * Provides the Update Set class, its builder, and its response.
 * @module CRDT
 */

/**
 * Command used tp update a set in Riak
 *
 * As a convenience, a builder class is provided:
 *
 *        var update = new UpdateSet.Builder()
 *               .withBucketType('sets')
 *               .withBucket('myBucket')
 *               .withKey('set_1')
 *               .withAdditions(['this', 'that', 'other'])
 *               .withCallback(callback)
 *               .build();
 *
 * See {{#crossLink "UpdateSet.Builder"}}UpdateSet.Builder{{/crossLink}}
 * @class UpdateSet
 * @constructor
 * @param {Object} options The options to use for this command.
 * @param {String} options.bucketType The bucket type in riak.
 * @param {String} options.bucket The bucket in riak.
 * @param {String} [options.key] The key for the set you want to store. Riak will generate one if not set.
 * @param {String[]|Buffer[]} [options.additions] The values to be added to the set.
 * @param {String[]|Buffer[]} [options.removals] The values to remove from the set. Note that a context is required.
 * @param {Buffer} [options.context] The context from a previous fetch. Required for remove operations.
 * @param {Boolean} [options.returnBody=true] Return the set.
 * @param {Boolean} [options.setsAsBuffers=false] Return the set as an array of Buffers rather than strings.
 * @param {Number} [options.w] The W value to use.
 * @param {Number} [options.dw] The DW value to use.
 * @param {Number} [options.pw] The PW value to use.
 * @param {Number} [options.timeout] Set a timeout for this command.
 * @param {Function} callback Rhe callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will ne null if no error.
 * @param {Object} callback.response The response from Riak. will be null if returnBody is not set.
 * @param {String} callback.response.generatedKey If no key was supplied, Riak will generate and return one here.
 * @param {Buffer} callback.response.context An opaque context to be used in any subsequent modification of the set.
 * @param {String[]|Buffer[]} callback.response.values An array holding the values in the set. String by default, Buffers if setsAsBuffers was used.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function UpdateSet(options, callback) {
    CommandBase.call(this, 'DtUpdateReq', 'DtUpdateResp', callback);
    this.validateOptions(options, schema);
}

inherits(UpdateSet, CommandBase);

function pbuf(self, prop) {
    return new Buffer(self.options[prop]);
}

function bufferize(thing) {
    if (utils.isString(thing)) {
        return new Buffer(thing);
    }
    else if (Buffer.isBuffer(thing)) {
        return thing;
    }
    else if (ByteBuffer.isByteBuffer(thing)) {
        return thing.toBuffer();
    }
    throw new Error("Couldn't bufferize " + (typeof thing) + " " + thing);
}

function buflist(list) {
    var newList = [];
    var len = list.length;
    for (var i = 0; i < len; i++) {
        newList[i] = bufferize(list[i]);
    }
    return newList;
}

UpdateSet.prototype.constructPbRequest = function() {
    var protobuf = this.getPbReqBuilder();

    // namespace
    protobuf.setType(pbuf(this, 'bucketType'));
    protobuf.setBucket(pbuf(this, 'bucket'));

    if (this.options.key) {
        protobuf.setKey(pbuf(this, 'key'));
    }

    // operation
    var op = new DtOp();
    protobuf.setOp(op);
    var setOp = new SetOp();
    op.setSetOp(setOp);
    setOp.adds = buflist(this.options.additions);
    setOp.removes = buflist(this.options.removals);

    // quorum
    protobuf.setW(this.options.w);
    protobuf.setDw(this.options.dw);
    protobuf.setPw(this.options.pw);

    // options
    protobuf.setReturnBody(this.options.returnBody);
    protobuf.setTimeout(this.options.timeout);
    protobuf.setContext(this.options.context);

    return protobuf;
};

UpdateSet.prototype.onSuccess = function(dtUpdateResp) {
    var response = null;

    // dtUpdateResp will be null if returnBody is not set
    if (dtUpdateResp) {
        var key = null;
        if (dtUpdateResp.getKey()) {
            key = dtUpdateResp.getKey().toString('utf8');
        }

        var valuesToReturn = new Array(dtUpdateResp.set_value.length);
        var i;
        for (i = 0; i < dtUpdateResp.set_value.length; i++) {
            if (this.options.setsAsBuffers) {
                valuesToReturn[i] = dtUpdateResp.set_value[i].toBuffer();
            } else {
                valuesToReturn[i] = dtUpdateResp.set_value[i].toString('utf8');
            }
        }

        response = { generatedKey: key,
                context: dtUpdateResp.getContext() ? dtUpdateResp.getContext().toBuffer() : null,
                values: valuesToReturn };
    }
    this._callback(null, response);
    return true;
};

var schema = Joi.object().keys({
    //namespace
    bucket: Joi.string().required(),
    // bucket type is required since default probably shouldn't have a
    // datatype associated with it
    bucketType: Joi.string().required(),
    // key is optional because Riak can generate one
    key: Joi.binary().default(null).optional(),

    //operations
    additions: Joi.array().default([]).optional(),
    removals: Joi.array().default([]).optional(),

    context: Joi.default(null).optional(),

    //quorum
    w: Joi.number().default(null).optional(),
    dw: Joi.number().default(null).optional(),
    pw: Joi.number().default(null).optional(),

    //options
    returnBody: Joi.boolean().default(true).optional(),
    timeout: Joi.number().default(null).optional(),
    setsAsBuffers: Joi.boolean().default(false).optional(),
});

/**
 * A builder for constructing UpdateSet instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a UpdateSet directly, this builder may be used.
 *
 *     var update = new UpdateSet.Builder()
 *                       .withBucketType('myBucketType')
 *                       .withBucket('myBucket')
 *                       .withKey('myKey')
 *                       .withAdditions(['this', 'that', 'other'])
 *                       .withCallback(myCallback)
 *                       .build();
 *
 * @class UpdateSet.Builder
 * @constructor
 */
function Builder() {}

Builder.prototype = {
    /**
     * Construct an UpdateSet instance.
     * @method build
     * @return {UpdateSet}
     */
    build: function() {
        var cb = this.callback;
        delete this.callback;
        return new UpdateSet(this, cb);
    }
};

/**
 * Set the callback.
 * @method withCallback
 * @param {Function} callback the callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will ne null if no error.
 * @param {Object} callback.response The response from Riak. will be null if returnBody is not set.
 * @param {String} callback.response.generatedKey If no key was supplied, Riak will generate and return one here.
 * @param {Buffer} callback.response.context An opaque context to be used in any subsequent modification of the set.
 * @param {String[]|Buffer[]} callback.response.values An array holding the values in the set. String by default, Buffers if setsAsBuffers was used.
 */
utils.bb(Builder, 'callback');

/**
* Set the bucket type.
* @method withBucketType
* @param {String} bucketType the bucket type in riak
* @chainable
*/
utils.bb(Builder, 'bucketType');

/**
* Set the bucket.
* @method withBucket
* @param {String} bucket the bucket in Riak
* @chainable
*/
utils.bb(Builder, 'bucket');

/**
* Set the key.
* If this is not set one will be generated by and returned from Riak
* @method withKey
* @param {String} key the key in riak.
* @chainable
*/
utils.bb(Builder, 'key');

/**
* Set the causal context. The context is necessary for set removals. It is
* an opaque field, and should not be parsed or modified.
* @method withContext
* @param {ByteBuffer} context the causal context
* @chainable
 */
utils.bb(Builder, 'context');

/**
* Return sets as arrays of Buffers rather than strings.
* By default the contents of sets are converted to strings. Setting this
* to true will cause this not to occur and the raw bytes returned
* as Buffer objects.
* @method withSetsAsBuffers
* @param {Boolean} setsAsBuffers true to not convert set contents to strings.
* @chainable
*/
utils.bb(Builder, 'setsAsBuffers');

/**
* Set the W value.
* If not set the bucket default is used.
* @method withW
* @param {Number} w the W value.
* @chainable
*/
utils.bb(Builder, 'w');

/**
* Set the DW value.
* If not set the bucket default is used.
* @method withDw
* @param {Number} dw the DW value.
* @chainable
*/
utils.bb(Builder, 'dw');

/**
* Set the PW value.
* If not set the bucket default is used.
* @method withPw
* @param {Number} pw the PW value.
* @chainable
*/
utils.bb(Builder, 'pw');

/**
* Set the return_body value.
* If true, the callback is passed the contents of the set after the update.
* @method withReturnBody
* @param {Boolean} returnBody the return_body value.
* @chainable
*/
utils.bb(Builder, 'returnBody');

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
utils.bb(Builder, 'useBasicQuorum');

/**
* Set a timeout for this operation.
* @method withTimeout
* @param {Number} timeout a timeout in milliseconds.
* @chainable
*/
utils.bb(Builder, 'timeout');

/**
 * The values you wish to add to this set.
 * @method withAdditions
 * @param {String[]|Buffer[]} additions The values to add.
 * @chainable
 */
utils.bb(Builder, 'additions');

/**
 * The values you wish to remove from this set.
 * __Note:__ when performing removals a context must be provided.
 * @method withRemovals
 * @param {String[]|Buffer[]} removals The values to remove.
 * @chainable
 */
utils.bb(Builder, 'removals');

module.exports = UpdateSet;
module.exports.Builder = Builder;
