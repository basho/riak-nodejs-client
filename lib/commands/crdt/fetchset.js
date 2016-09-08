'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');

var utils = require('../../utils');

/**
 * Provides the FetchCounter class, its builder, and its response.
 * @module CRDT
 */

/**
 *  Command used to fetch a set from Riak.
 *
 *  As a convenience, a builder class is provided:
 *
 *      var fetch = new FetchSet.Builder()
 *                      .withBucketType('myBucketType')
 *                      .withBucket('myBucket')
 *                      .withKey('myKey')
 *                      .withCallback(myCallback)
 *                      .build();
 *
 * See {{#crossLink "FetchSet.Builder"}}FetchSet.Builder{{/crossLink}}
 *
 * @class FetchSet
 * @constructor
 * @param {Object} options The options to use for this command.
 * @param {String} options.bucketType The bucket type in riak.
 * @param {String} options.bucket The bucket in riak.
 * @param {String} options.key The key for the counter you want to fetch.
 * @param {Boolean} [options.setsAsBuffers=false] Return values as Buffers rather than strings.
 * @param {Number} [options.timeout] Tet a timeout for this operation.
 * @param {Number} [options.r] The R value to use for this fetch.
 * @param {Number} [options.pr] The PR value to use for this fetch.
 * @param {Boolean} [options.notFoundOk] If true a vnode returning notfound for a key increments the r tally.
 * @param {Boolean} [options.useBasicQuorum] Controls whether a read request should return early in some fail cases.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response The response from Riak.
 * @param {Buffer} callback.response.context An opaque context to be used in any subsequent modification of the set.
 * @param {String[]|Buffer[]} callback.response.values An array holding the values in the set. String by default, Buffers if setsAsBuffers was used.
 * @param {Boolean} callback.response.isNotFound True if there was no set in Riak.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */

function FetchSet(options, callback) {
    CommandBase.call(this, 'DtFetchReq', 'DtFetchResp', callback);
    this.validateOptions(options, schema);
}

inherits(FetchSet, CommandBase);

function buf(self, prop) {
    return new Buffer(self.options[prop]);
}

FetchSet.prototype.constructPbRequest = function() {
    var protobuf = this.getPbReqBuilder();

    // namespace
    protobuf.setType(buf(this, 'bucketType'));
    protobuf.setBucket(buf(this, 'bucket'));
    protobuf.setKey(buf(this, 'key'));

    // quorum
    protobuf.setR(this.options.r);
    protobuf.setPr(this.options.pr);
    protobuf.setNotfoundOk(this.options.notFoundOk);
    protobuf.setBasicQuorum(this.options.useBasicQuorum);

    protobuf.setTimeout(this.options.timeout);

    return protobuf;
};

FetchSet.prototype.onSuccess = function(dtFetchResp) {

    var response;
    var isNotFound = false;
    // on a "not found" the value is null
    var valuesToReturn = null;
    var context = null;
    if (dtFetchResp.getValue()) {
        var valueBuffers = dtFetchResp.getValue().getSetValue();
        valuesToReturn = new Array(valueBuffers.length);
        var valueCount = valueBuffers.length;
        for (var i = 0; i < valueCount; i++) {
            if (this.options.setsAsBuffers) {
                valuesToReturn[i] = valueBuffers[i].toBuffer();
            } else {
                valuesToReturn[i] = valueBuffers[i].toString('utf8');
            }
        }

        context = dtFetchResp.getContext() ? dtFetchResp.getContext().toBuffer() : null;

    } else {
        valuesToReturn = [];
        isNotFound = true;
    }

    // TODO 2.0 - remove notFound
    response = { context: context, values: valuesToReturn, isNotFound: isNotFound, notFound: isNotFound };

    this._callback(null, response);
    return true;
};

var schema = Joi.object().keys({
    // namespace
    bucket: Joi.string().required(),
    // bucket type is required since default probably shouldn't have a
    // datatype associated with it
    bucketType: Joi.string().required(),
    key: Joi.binary().required(),
    returnRaw: Joi.boolean().default(false).optional(),

    // quorum
    r: Joi.number().default(null).optional(),
    pr: Joi.number().default(null).optional(),
    notFoundOk: Joi.boolean().default(null).optional(),
    useBasicQuorum: Joi.boolean().default(null).optional(),

    setsAsBuffers: Joi.boolean().default(false).optional(),
    timeout: Joi.number().default(null).optional()

});

/**
 * A builder for constructing FetchSet instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a FetchSet directly, this builder may be used.
 *
 *     var fetch = new FetchSet.Builder()
 *                      .withBucketType('myBucketType')
 *                      .withBucket('myBucket')
 *                      .withKey('myKey')
 *                      .withCallback(myCallback)
 *                      .build();
 *
 * @class FetchSet.Builder
 * @constructor
 */
function Builder() {}

Builder.prototype = {
    /**
     * Construct a SetchSet instance.
     * @method build
     * @return {FetchSet}
     */
    build: function() {
        var cb = this.callback;
        delete this.callback;
        return new FetchSet(this, cb);
    }
};

/**
* Set the callback to be executed when the operation completes.
* @method withCallback
* @param {String} callback.err An error message. Will be null if no error.
* @param {Object} callback.response The response from Riak.
* @param {Buffer} callback.response.context An opaque context to be used in any subsequent modification of the set.
* @param {String[]|Buffer[]} callback.response.values An array holding the values in the set. String by default, Buffers if setsAsBuffers was used.
* @param {Boolean} callback.response.isNotFound True if there was no set in Riak.
* @chainable
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
* @method withKey
* @param {String} key the key in riak.
* @chainable
*/
utils.bb(Builder, 'key');

/**
* Set the R value.
* If not set the bucket default is used.
* @method withR
* @param {Number} r the R value.
* @chainable
*/
utils.bb(Builder, 'r');

/**
* Set the PR value.
* If not set the bucket default is used.
* @method withPr
* @param {Number} pr the PR value.
* @chainable
*/
utils.bb(Builder, 'pr');

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
* Set the not_found_ok value.
* If true a vnode returning notfound for a key increments the r tally.
* False is higher consistency, true is higher availability.
* If not asSet the bucket default is used.
* @method withNotFoundOk
* @param {Boolean} notFoundOk the not_found_ok value.
* @chainable
*/
utils.bb(Builder, 'notFoundOk');

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

module.exports = FetchSet;
module.exports.Builder = Builder;
