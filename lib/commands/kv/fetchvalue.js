/*
 * Copyright 2015 Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


var RiakObject = require('./riakobject');
var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');

/**
 * Provides the FetchValue class, its builder, and its response.
 * @module KV
 */


/**
 * Command used to fetch an object from Riak.
 *
 * As a convenience, a builder class is provided:
 *
 *     var fetch = new FetchValue.Builder()
 *         .withBucket('myBucket')
 *         .withKey('myKey')
 *         .withCallback(myCallback)
 *         .build();
 *
 * See {{#crossLink "FetchValue.Builder"}}FetchValue.Builder{{/crossLink}}
 *
 * @class FetchValue
 * @constructor
 * @param {Object} options The options for this command.
 * @param {String} [options.bucketType=default] The bucket type in riak. If not supplied 'default' is used.
 * @param {String} options.bucket The bucket in riak.
 * @param {String} options.key The key for the object you want to fetch.
 * @param {Boolean} [options.convertToJs=false] Convert the values stored in riak to a JS object using JSON.parse()
 * @param {Function} [options.conflictResolver] A function used to resolve siblings to a single object.
 * @param {RiakObject[]|Object[]} options.conflictResolver.objects The array of objects returned from Riak.
 * @param {Number} [options.timeout] Set a timeout for this operation.
 * @param {Number} [options.r] The R value to use for this fetch.
 * @param {Number} [options.pr] The PR value to use for this fetch.
 * @param {Boolean} [options.notFoundOk] If true a vnode returning notfound for a key increments the r tally.
 * @param {Boolean} [options.useBasicQuorum] Controls whether a read request should return early in some fail cases.
 * @param {Boolean} [options.returnDeletedVClock] True to return tombstones.
 * @param {Boolean} [options.headOnly] Return only the metadata.
 * @param {Buffer} [options.ifNotModified] Do not return the object if the supplied vclock matches.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will ne null if no error.
 * @param {Object} callback.response the response from Riak.
 * @param {Boolean} callback.response.isNotFound True if there was no value in Riak.
 * @param {Boolean} callback.response.isUnchanged True if the object has not changed (based on a vclock provided via ifNotModified)
 * @param {Buffer} callback.response.vclock The vector clock for this object (and its siblings)
 * @param {Object[]|RiakObject[]} callback.response.values An array of one or more values. Either RiakObjects or JS objects if convertToJs was used.
 * @extends CommandBase
 *
 */
function FetchValue(options, callback) {

    CommandBase.call(this, 'RpbGetReq', 'RpbGetResp', callback);

    var self = this;
    Joi.validate(options, schema, function(err, options) {

        if (err) {
            throw err;
        }

        self.options = options;
    });

    this.streaming = false;
    this.remainingTries = 1;
}

inherits(FetchValue, CommandBase);

FetchValue.prototype.constructPbRequest = function() {

    var protobuf = this.getPbReqBuilder();

    protobuf.setBucket(new Buffer(this.options.bucket));
    protobuf.setType(new Buffer(this.options.bucketType));
    protobuf.setKey(new Buffer(this.options.key));

    protobuf.setR(this.options.r);
    protobuf.setPr(this.options.pr);
    protobuf.setNotfoundOk(this.options.notFoundOk);
    protobuf.setBasicQuorum(this.options.useBasicQuorum);
    protobuf.setDeletedvclock(this.options.returnDeletedVClock);
    protobuf.setHead(this.options.headOnly);
    protobuf.setIfModified(this.options.ifNotModified);
    protobuf.setTimeout(this.options.timeout);

    return protobuf;

};

FetchValue.prototype.parseResponse = function(rpbGetResp) {
    // If the response is null ... it means not found. Riak only sends
    // a message code and zero bytes when that's the case.
    // Because that makes sense!
    var response;
    if (rpbGetResp) {

        var pbContentArray = rpbGetResp.getContent();
        var vclock = rpbGetResp.getVclock().toBuffer();
        var unchanged = rpbGetResp.getUnchanged();
        // To unify the behavior of having just a tombstone vs. siblings
        // that include a tombstone, we create an empty object and mark
        // it deleted
        var riakMeta, riakValue, riakObject;
        if (pbContentArray.length === 0) {

            riakObject = new RiakObject();

            riakObject.isTombstone = true;
            riakObject.key = this.key;
            riakObject.bucket = this.bucket;
            riakObject.bucketType = this.bucketType;

            response = { isNotFound : false, isUnchanged : unchanged, vclock: vclock, values : [riakObject] };

        } else {

            var values = new Array(pbContentArray.length);

            for (var i = 0; i < pbContentArray.length; i++) {

                riakObject = RiakObject.createFromRpbContent(pbContentArray[i], this.options.convertToJs);
                riakObject.vclock = vclock;
                riakObject.bucket = this.options.bucket;
                riakObject.bucketType = this.options.bucketType;
                riakObject.key = this.options.key;

                values[i] = riakObject;
            }

            if (this.options.conflictResolver) {
                values = [this.options.conflictResolver(values)];
            }

            response = { isNotFound: false, isUnchanged: unchanged, vclock: vclock, values: values };
        }
    } else {
        response = { isNotFound: true, isUnchanged: false, vclock: null, values: [] };
    }
    return response;
};

FetchValue.prototype.onSuccess = function(rpbGetResp) {
    var response;
    try{
        response = this.parseResponse(rpbGetResp);
    }
    catch (err){
        return this._callback(err);
    }

    this._callback(null, response);

    return true;
};

var schema = Joi.object().keys({
   bucket: Joi.string().required(),
   bucketType: Joi.string().default('default'),
   key: Joi.string().required(),
   r: Joi.number().default(null).optional(),
   pr: Joi.number().default(null).optional(),
   notFoundOk: Joi.boolean().default(false).optional(),
   useBasicQuorum: Joi.boolean().default(false).optional(),
   returnDeletedVClock: Joi.boolean().default(false).optional(),
   headOnly: Joi.boolean().default(false).optional(),
   ifNotModified: Joi.binary().default(null).optional(),
   timeout: Joi.number().default(null).optional(),
   conflictResolver: Joi.func().default(null).optional(),
   convertToJs: Joi.boolean().default(false).optional()
});

/**
 * A builder for constructing FetchValue instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a FetchValue directly, this builder may be used.
 *
 *     var fetchValue = new FetchValue.Builder()
 *          .withBucket('myBucket')
 *          .withKey('myKey')
 *          .build();
 *
 * @class FetchValue.Builder
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
    },
    /**
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
        this.useBasicQuorum = useBasicQuorum;
        return this;
    },
    /**
    * Set whether to return tombstones.
    * @method withReturnDeletedVClock
    * @param {Boolean} returnDeletedVClock true to return tombstones, false otherwise.
    * @chainable
    */
    withReturnDeletedVClock : function(returnDeletedVClock) {
        this.returnDeletedVClock = returnDeletedVClock;
        return this;
    },
    /**
    * Return only the metadata.
    * Causes Riak to only return the metadata for the object. The value
    * will be asSet to null.
    * @method withHeadOnly
    * @param {Boolean} headOnly true to return only metadata.
    * @chainable
    */
    withHeadOnly : function(headOnly) {
        this.headOnly = headOnly;
        return this;
    },
    /**
    * Do not return the object if the supplied vclock matches.
    * @method withIfNotModified
    * @param {Buffer} vclock the vclock to match on
    * @chainable
    */
    withIfNotModified : function(vclock) {
        this.ifNotModified = vclock;
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
     * @param {Function} callback The callback to be executed when the operation completes.
     * @param {String} callback.err An error message. Will ne null if no error.
     * @param {Object} callback.response the response from Riak.
     * @param {Boolean} callback.response.isNotFound True if there was no value in Riak.
     * @param {Boolean} callback.response.isUnchanged True if the object has not changed (based on a vclock provided via ifNotModified)
     * @param {Buffer} callback.response.vclock The vector clock for this object (and its siblings)
     * @param {Object[]|RiakObject[]} callback.response.values An array of one or more values. Either RiakObjects or JS objects if convertToJs was used.
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
    /**
     * Provide a conflict resolver to resolve siblings.
     * If siblings are present Riak will return all of them. The provided
     * function will be used to resolve these to a single response.
     *
     * If a conflict resolver is not provided all siblings will be returned.
     * @method withConflictResolver
     * @param {Function} conflictResolver - the conflict resolver to be used.
     * @chainable
     */
    withConflictResolver : function(conflictResolver) {
        this.conflictResolver = conflictResolver;
        return this;
    },
    /**
     * Convert the value stored in Riak to a JS object.
     * Values are stored in Riak as bytes. Setting this to true will
     * convert the value to a JS object using JSON.parse() before
     * passing them to the conflict resolver.
     * @method withConvertValueToJs
     * @param {Boolean} convert - true to convert the value(s), false otherwise.
     * @chainable
     */
    withConvertValueToJs : function(convertToJs) {
        this.convertToJs = convertToJs;
        return this;
    },
    /**
     * Construct a FetchValue command.
     * @method build
     * @return {FetchValue}
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new FetchValue(this, cb);
    }

};

module.exports = FetchValue;
module.exports.Builder = Builder;

