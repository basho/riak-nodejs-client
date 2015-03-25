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

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');

/**
 * Provides the FetchCounter class, its builder, and its response.
 * @module FetchSet
 */

/**
 *  Command used to fetch a set  from Riak.
 *  
 *  As a convenience, a builder class is provided:
 *      var FetchSet = require('./lib/commands/crdt/fetchset');
 *      var fetch = new FetchSet.Builder()
 *                      .withBucketType('myBucketType')
 *                      .withBucket('myBucket')
 *                      .withKey('myKey')
 *                      .withCallback(myCallback)
 *                      .build();
 *                      
 *
 * @class FetchSet
 * @constructor
 * @param {Object} options the options to use for this command.
 * @param {String} options.bucketType the bucket type in riak.
 * @param {String} options.bucket the bucket in riak.
 * @param {String} options.key the key for the counter you want to fetch.
 * @param {Function} options.callback the callback to be executed when the operation completes.
 * @param {String} options.callback.err An error message
 * @param {Object} options.callback.response the set from Riak. null if not found.
 * @param {Number} [options.timeout] set a timeout for this operation.
 * @param {Number} [options.r] the R value to use for this fetch.
 * @param {Number} [options.pr] the PR value to use for this fetch.
 * @param {Boolean} [options.notFoundOk] if true a vnode returning notfound for a key increments the r tally.
 * @param {Boolean} [options.useBasicQuorum] controls whether a read request should return early in some fail cases.
 * @extends CommandBase 
 */

function FetchSet(options) {
    CommandBase.call(this, 'DtFetchReq', 'DtFetchResp');

    var self = this;
    Joi.validate(options, schema, function(err, options) {
        if (err) {
            throw err;
        }
        self.options = options;
    });
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
    
    var response = null;
    // on a "not found" the value is null
    if (dtFetchResp.getValue())
    {
        var valueBuffers = dtFetchResp.getValue().getSetValue();
        var valueStrings = new Array(valueBuffers.length);
        var valueCount = valueBuffers.length;
        for (var i = 0; i < valueCount; i++) {
            valueStrings[i] = valueBuffers[i].toString('utf8');
        }
        var response = {
            // treat context as opaque, don't string-ify
            context: dtFetchResp.getContext(),
            dataType: dtFetchResp.getType(),
            valueBuffers: valueBuffers,
            value: valueStrings
        };
    }
    this.options.callback(null, response);
    return true;
};

FetchSet.prototype.onRiakError = function(rpbErrorResp) {
    this.onError(rpbErrorResp.getErrmsg().toString('utf8'));
};

FetchSet.prototype.onError = function(msg) {
    this.options.callback(msg, null);
};

var schema = Joi.object().keys({
    callback: Joi.func().required(),

    // namespace
    bucket: Joi.string().required(),
    // bucket type is required since default probably shouldn't have a
    // datatype associated with it
    bucketType: Joi.string().required(),
    key: Joi.string().required(),
    returnRaw: Joi.boolean().default(false).optional(),

    // quorum
    r: Joi.number().default(null).optional(),
    pr: Joi.number().default(null).optional(),
    notFoundOk: Joi.boolean().default(null).optional(),
    useBasicQuorum: Joi.boolean().default(null).optional(),

    timeout: Joi.number().default(null).optional()

});

/**
 * A builder for constructing FetchSet instances.
 * * Rather than having to manually construct the __options__ and instantiating
 * a FetchSet directly, this builder may be used.
 * 
 *      var FetchSet = require('./lib/commands/datatype/fetchset');
 *      var fetch = new FetchSet.Builder()
 *                      .withBucketType('myBucketType')
 *                      .withBucket('myBucket')
 *                      .withKey('myKey')
 *                      .withCallback(myCallback)
 *                      .build();
 *       
 * @namespace FetchSet
 * @class Builder
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
        return new FetchSet(this);
    }
};

function firstUc(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}

function bb(prop) {
    Builder.prototype["with"+firstUc(prop)] = function(pv) {
        this[prop] = pv;
        return this;
    };
}

bb('callback');
/**
* Set the bucket type.
* @method withBucketType
* @param {String} bucketType the bucket type in riak
* @chainable
*/
bb('bucketType');
/**
* Set the bucket.
* @method withBucket
* @param {String} bucket the bucket in Riak
* @chainable
*/
bb('bucket');
/**
* Set the key.
* @method withKey
* @param {String} key the key in riak.
* @chainable
*/
bb('key');
/**
* Set the R value.
* If not set the bucket default is used.
* @method withR
* @param {Number} r the R value.
* @chainable
*/
bb('r');
/**
* Set the PR value.
* If not set the bucket default is used.
* @method withPr
* @param {Number} pr the PR value.
* @chainable
*/
bb('pr');
/**
* Set the not_found_ok value.
* If true a vnode returning notfound for a key increments the r tally.
* False is higher consistency, true is higher availability.
* If not asSet the bucket default is used.
* @method withNotFoundOk
* @param {Boolean} notFoundOk the not_found_ok value.
* @chainable
*/
bb('notFoundOk');
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
bb('useBasicQuorum');
/**
* Set a timeout for this operation.
* @method withTimeout
* @param {Number} timeout a timeout in milliseconds.
* @chainable
*/
bb('timeout');


module.exports = FetchSet;
module.exports.Builder = Builder;
