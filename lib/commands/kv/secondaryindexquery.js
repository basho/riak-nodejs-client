'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');
var RpbIndexReq = require('../../protobuf/riakprotobuf').getProtoFor('RpbIndexReq');
var RpbPair = require('../../protobuf/riakprotobuf').getProtoFor('RpbPair');
var RiakObject = require('./riakobject');

/**
 * Provides the SecondaryIndexQuery class and its builder.
 * @module KV
 */

/**
 * Command used to perform a secondary index query.
 *
 * As a convenience, a builder class is provided:
 *
 *     var query = new SecondaryIndexQuery.Builder()
 *                  .withBucket('myBucket')
 *                  .withIndexName('email_bin')
 *                  .withIndexKey('roach@basho.com')
 *                  .withCallback(myCallback)
 *                  .build();
 *
 * See {{#crossLink "SecondaryIndexQuery.Builder"}}SecondaryIndexQuery.Builder{{/crossLink}}
 *
 * @class SecondaryIndexQuery
 * @constructor
 * @param {Object} options The options for this command.
 * @param {String} [options.bucketType=default] The bucket type in riak. If not supplied 'default' is used.
 * @param {String} options.bucket The bucket in riak.
 * @param {String} options.indexName The secondary index name to query
 * @param {String|Number} [options.indexKey] A single index key to query
 * @param {String|Number} [options.rangeStart] Starting key for a range query
 * @param {String|Number} [options.rangeEnd] Ending key for a range query
 * @param {Boolean} [options.returnKeyAndIndex=false] Return the index keys along with the object keys
 * @param {Number} [options.maxResults] Limit the results returned and paginate if necessary
 * @param {Buffer} [options.continuation] A continuation returned from a previous query that set maxResults. Used for pagination.
 * @param {Boolean} [options.stream=true] Whether to stream or accumulate the result before calling callback.
 * @param {Number} [options.timeout] Set a timeout for this operation.
 * @param {Boolean} [options.paginationSort=false] True to sort a non-paginated query.
 * @param {Function} callback the callback to be executed by the command.
 * @param {String} callback.err  An error message. Will ne null if no error.
 * @param {Object} callback.response The response from Riak.
 * @param {Object[]} callback.response.values Object keys returned by the query, and optionally the index keys.
 * @param {Boolean} callback.response.done True if you have received all the results.
 * @param {Buffer} callback.response.continuation The continuation if a continuation was returned.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function SecondaryIndexQuery(options, callback) {
    CommandBase.call(this, 'RpbIndexReq', 'RpbIndexResp', callback);
    this.validateOptions(options, schema);

    if (this.options.indexKey === null && (this.options.rangeStart === null || this.options.rangeEnd === null)) {
        throw new Error("either 'indexKey' or 'rangeStart' + 'rangeEnd' are required");
    }

    if (!this.options.stream) {
        this.responses = [];
    }
}

inherits(SecondaryIndexQuery, CommandBase);

SecondaryIndexQuery.prototype.constructPbRequest = function() {

    var protobuf = this.getPbReqBuilder();
    // We always stream from Riak.
    protobuf.setStream(true);

    protobuf.setBucket(new Buffer(this.options.bucket));
    protobuf.setType(new Buffer(this.options.bucketType));
    protobuf.setIndex(new Buffer(this.options.indexName));
    protobuf.setReturnTerms(this.options.returnKeyAndIndex);

    if (this.options.indexKey !== null) {
        // _int index requests take ... strings, not numbers
        protobuf.setKey(new Buffer(this.options.indexKey.toString()));
        protobuf.setQtype(RpbIndexReq.IndexQueryType.eq);
    } else {
        protobuf.setRangeMin(new Buffer(this.options.rangeStart.toString()));
        protobuf.setRangeMax(new Buffer(this.options.rangeEnd.toString()));
        protobuf.setQtype(RpbIndexReq.IndexQueryType.range);
    }

    if (!this.options.maxResults && this.options.paginationSort) {
        protobuf.setPaginationSort(this.options.paginationSort);
    }
    protobuf.setMaxResults(this.options.maxResults);
    protobuf.setContinuation(this.options.continuation);
    protobuf.setTimeout(this.options.timeout);

    return protobuf;
};

SecondaryIndexQuery.prototype.onSuccess = function(rpbIndexResp) {

    /*
    * @private
    * The 2i API is inconsistent on the Riak side. If it's not
    * a range query, return_terms is ignored it only returns the
    * list of object keys and you have to have
    * preserved the index key if you want to return it to the user
    * with the results.
    *
    * Also, the $key index queries just ignore return_terms altogether.
    */

    var i, resultsToReturn;
    if (rpbIndexResp.results.length) {
        // Index keys and object keys were returned
        resultsToReturn = new Array(rpbIndexResp.results.length);
        for (i = 0; i < rpbIndexResp.results.length; i++) {
            var iKey = rpbIndexResp.results[i].key.toString('utf8');
            if (RiakObject.isIntIndex(this.options.indexName)) {
                iKey = parseInt(iKey);
            }
            resultsToReturn[i] = { indexKey : iKey,
                                  objectKey : rpbIndexResp.results[i].value.toString('utf8') };
        }
    } else {
        // only object keys were returned
        resultsToReturn = new Array(rpbIndexResp.keys.length);
        for (i = 0; i < rpbIndexResp.keys.length; i++) {
            var key = null;
            if (this.options.returnKeyAndIndex) {
                // this is only possible if this was a single key query
                key = this.options.indexKey;
            }
            resultsToReturn[i] = { indexKey: key, objectKey: rpbIndexResp.keys[i].toString('utf8') };
        }
    }

    var continuation = rpbIndexResp.continuation;

    if (this.options.stream) {
        this._callback(null, { values: resultsToReturn, done: rpbIndexResp.done, continuation: continuation });
    } else {
        Array.prototype.push.apply(this.responses, resultsToReturn);
        if (rpbIndexResp.done) {
            this._callback(null, { values: this.responses, done : true, continuation: continuation });
        }
    }

    return rpbIndexResp.done;

};

// Explicitly set null on options for protobufs and for conditionals
var schema = Joi.object().keys({
    bucket: Joi.string().required(),
    bucketType: Joi.string().default('default'),
    indexName: Joi.string().required(),
    indexKey: Joi.alternatives().try(
            Joi.number().options({ convert: false }), Joi.string()
        ).default(null).optional(),
    rangeStart: Joi.alternatives().try(
            Joi.number().options({ convert: false }), Joi.string()
        ).default(null).optional(),
    rangeEnd: Joi.alternatives().try(
            Joi.number().options({ convert: false }), Joi.string()
        ).default(null).optional(),
    returnKeyAndIndex: Joi.boolean().default(null).optional(),
    maxResults: Joi.number().default(null).optional(),
    continuation: Joi.any().default(null).optional(),
    timeout: Joi.number().default(null).optional(),
    stream: Joi.boolean().default(true).optional(),
    paginationSort: Joi.boolean().default(null).optional()
});

/**
 * A builder for constructing SecondaryIndexquery instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a SecondaryIndexQuery directly, this builder may be used.
 *
 *     var query = new SecondaryIndexQuery.Builder()
 *                  .withBucket('myBucket')
 *                  .withIndexName('email_bin')
 *                  .withIndexKey('roach@basho.com')
 *                  .withCallback(myCallback)
 *                  .build();
 *
 * @class SecondaryIndexQuery.Builder
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
     * Set the index name
     * @method
     * @param {String} indexName the index to query
     * @chainable
     */
    withIndexName : function(indexName) {
        this.indexName = indexName;
        return this;
    },
    /**
     * Set a single secondary index key to use for query.
     * Note that you can only set a single key, or a range.
     * @method withIndexKey
     * @param {String|Number} indexKey the secondary index key.
     * @chainable
     */
    withIndexKey : function(indexKey) {
        this.indexKey = indexKey;
        return this;
    },
    /**
     * Set a range for the query
     * @method withRange
     * @param {String|Number} start the start of the range
     * @param {String|Number} end the end of the range
     * @chainable
     */
    withRange : function(start, end) {
        this.rangeStart = start;
        this.rangeEnd = end;
        return this;
    },
    /**
     * Set whether to return the index keys with the Riak object keys.
     * Setting this to true will return both the index key and the Riak
     * object's key. The default is false (only to return the Riak object keys).
     * @method withReturnKeyAndIndex
     * @param {Boolean} returnKeyAndIndex whether to return the index keys as well
     * @chainable
     */
    withReturnKeyAndIndex : function(returnKeyAndIndex) {
        this.returnKeyAndIndex = returnKeyAndIndex;
        return this;
    },
    /**
     * Set the maximum number of results returned by the query.
     *
     * When asking for large result sets, it is often desirable to ask the
     * servers to return chunks of results instead of a firehose.
     * You can do so using this method, where maxResults is the number of
     * results you'd like to receive.
     *
     * Assuming more keys are available, a continuation value will be included
     * in the results to allow the client to request the next page.
     * @method withMaxResults
     * @param {Number} maxResults the max number of results to return.
     * @chainable
     */
    withMaxResults : function(maxResults) {
        this.maxResults = maxResults;
        return this;
    },
    /**
     * Set the continuation for this query.
     * If using pagination via maxResults, this will have been returned
     * by a previous query.
     * @method withContinuation
     * @param {Buffer} continuation the continuation from a previous query
     * @chainable
     */
    withContinuation : function(continuation) {
        this.continuation = continuation;
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
     * @param {Function} callback the callback to be executed by the command.
     * @param {String} callback.err  An error message. Will ne null if no error.
     * @param {Object} callback.response The response from Riak.
     * @param {Object[]} callback.response.values Object keys returned by the query, and optionally the index keys.
     * @param {Boolean} callback.response.done True if you have received all the results.
     * @param {Buffer} callback.response.continuation The continuation if a continuation was returned.
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
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
     * Set whether to sort the results of a non-paginated 2i query.
     * If you are not using pagination (setting withMaxResults()) the
     * default behavior in Riak is to not sort the result set.
     * Setting this to true will sort the results before returning them.
     * __Note that this is not recommended for queries that could return a large
     * result set; the overhead in Riak is substantial. __
     * @method withPaginationSort
     * @param {Boolean} paginationSort true to sort the results of a non-paginated query
     * @chainable
     */
    withPaginationSort : function(paginationSort) {
        this.paginationSort = paginationSort;
        return this;
    },
    /**
     * Construct a SecondaryIndexQuery instance
     * @method build
     * @return {SecondaryIndexQuery}
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new SecondaryIndexQuery(this, cb);
    }

};

module.exports = SecondaryIndexQuery;
module.exports.Builder = Builder;

