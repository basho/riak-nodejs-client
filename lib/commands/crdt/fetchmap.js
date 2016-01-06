'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');
var DtFetchResp = require('../../protobuf/riakprotobuf').getProtoFor('DtFetchResp');
var MapField = require('../../protobuf/riakprotobuf').getProtoFor('MapField');

/**
 * Provides the FetchMap class, its builder, and its response.
 * @module CRDT
 */

/**
 * Command for fetching a map from Riak.
 *
 * As a convenience, a builder class is provided:
 *
 *     var fetch = new FetchMap.Builder()
 *                      .withBucketType('myBucketType')
 *                      .withBucket('myBucket')
 *                      .withKey('myKey')
 *                      .withCallback(myCallback)
 *                      .build();
 *
 * See {{#crossLink "FetchMap.Builder"}}FetchMap.Builder{{/crossLink}}
 *
 * @class FetchMap
 * @constructor
 * @param {Object} options The options to use for this command.
 * @param {String} options.bucketType The bucket type in riak.
 * @param {String} options.bucket The bucket in riak.
 * @param {String} options.key The key for the map you want to fetch.
 * @param {Boolean} [options.setsAsBuffers=false] Return sets in the map as arrays of Buffers rather than strings.
 * @param {Number} [options.timeout] Set a timeout for this operation.
 * @param {Number} [options.r] The R value to use for this operation.
 * @param {Number} [options.pr] The PR value to use for this operation.
 * @param {Boolean} [options.notFoundOk] If true a vnode returning notfound for a key increments the r tally.
 * @param {Boolean} [options.useBasicQuorum] Controls whether a read request should return early in some fail cases.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response The response from Riak.
 * @param {Buffer} callback.response.context An opaque context to be used in any subsequent modification of the map.
 * @param {Object} callback.response.map The map in Riak, converted to a JS object.
 * @param {Boolean} callback.response.isNotFound True if there was no map in Riak.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 * @constructor
 */
function FetchMap(options, callback) {
    CommandBase.call(this, 'DtFetchReq', 'DtFetchResp', callback);
    this.validateOptions(options, schema);
}

inherits(FetchMap, CommandBase);

FetchMap.prototype.constructPbRequest = function() {

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

FetchMap.prototype.onSuccess = function(dtFetchResp) {
    var response;
    // on "not found" dtFetchResp.value will be null
    if (dtFetchResp.getValue()) {
        if (dtFetchResp.getType() !== DtFetchResp.DataType.MAP) {
            this.onError('Requested map, received ' + dtType[dtFetchResp.getType()]);
        } else {
            // TODO 2.0 - remove notFound
            response = { context: dtFetchResp.getContext().toBuffer(),
                map: FetchMap.parsePbResponse(dtFetchResp.value.map_value, this.options.setsAsBuffers),
                isNotFound: false, notFound: false };
        }
    } else {
        // TODO 2.0 - remove notFound
        response = { context: null, map: null, isNotFound: true, notFound: true };
    }
    this._callback(null, response);
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
   timeout: Joi.number().default(null).optional(),
   setsAsBuffers: Joi.boolean().default(false).optional()
});

var dtType = {
    1: 'counter',
    2: 'set',
    3: 'map'
};

/**
 * A builder for constructing FetchMap instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a FetchMap directly, this builder may be used.
 *
 *     var fetch = new FetchMap.Builder()
 *                     .withBucketType('myBucketType')
 *                     .withBucket('myBucket')
 *                     .withKey('myKey')
 *                     .withCallback(myCallback)
 *                     .build();
 *
 * @class FetchMap.Builder
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
     * Return sets as arrays of Buffers rather than strings.
     * By default the contents of sets are converted to strings. Setting this
     * to true will cause this not to occur and the raw bytes returned
     * as Buffer objects.
     * @method withSetsAsBuffers
     * @param {Boolean} setsAsBuffers true to not convert set contents to strings.
     * @chainable
     */
    withSetsAsBuffers : function(setsAsBuffers) {
        this.setsAsBuffers = setsAsBuffers;
        return this;
    },
    /**
     * Set the R value.
     * If not set the bucket default is used.
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
    * If not set the bucket default is used.
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
     * @param {Function} callback The callback to be executed when the operation completes.
     * @param {String} callback.err An error message. Will be null if no error.
     * @param {Object} callback.response The response from Riak. If no map is in Riak for the key, values will be null.
     * @param {Buffer} callback.response.context An opaque context to be used in any subsequent modification of the map.
     * @param {Object} callback.response.map The map in Riak, converted to a JS object.
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
    /**
     * Construct a FetchMap instance.
     * @method build
     * @return {FetchMap}
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new FetchMap(this, cb);
    }

};

module.exports = FetchMap;
module.exports.Builder = Builder;

module.exports.parsePbResponse = function(pbMapEntries, setsAsBuffers) {

    var map = { counters: {}, sets: {}, registers: {}, flags: {}, maps: {} };
    var i, j;
    for (i = 0; i < pbMapEntries.length; i++) {
        var mapEntry = pbMapEntries[i];
        var mapField = mapEntry.getField();
        var key = mapField.name.toString('utf8');
        switch(mapField.type) {
            case MapField.MapFieldType.COUNTER:
                map.counters[key] = mapEntry.getCounterValue().toNumber();
                break;
            case MapField.MapFieldType.SET:
                var setToReturn = new Array(mapEntry.set_value.length);
                for (j = 0; j < mapEntry.set_value.length; j++) {
                    if (setsAsBuffers) {
                        setToReturn[j] = mapEntry.set_value[j].toBuffer();
                    } else {
                        setToReturn[j] = mapEntry.set_value[j].toString('utf8');
                    }
                }
                map.sets[key] = setToReturn;
                break;
            case MapField.MapFieldType.REGISTER:
                map.registers[key] = mapEntry.getRegisterValue().toBuffer();
                break;
            case MapField.MapFieldType.FLAG:
                map.flags[key] = mapEntry.getFlagValue();
                break;
            case MapField.MapFieldType.MAP:
                map.maps[key] = FetchMap.parsePbResponse(mapEntry.map_value);
                break;
            default:
                break;
        }
    }
    return map;
};
