'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');

/**
 * Provides the (Yokozuna) FetchIndex command.
 * @module YZ
 */

/**
 * Command used to fetch a (Yokozuna) index or all indexes.
 *
 * As a convenience, a builder class is provided:
 *
 *     var fetch = FetchIndex.Builder()
 *                  .withIndexName('index_name')
 *                  .withCallback(callback)
 *                  .build();
 *
 * See {{#crossLink "FetchIndex.Builder"}}FetchIndex.Builder{{/crossLink}}
 *
 * @class FetchIndex
 * @constructor
 * @param {Object} options the options for this command
 * @param {String} [options.indexName] the name of a specific index to fetch. If not supplied, all are returned.
 * @param {Function} callback the callback to be executed when the operation completes.
 * @param {String} callback.err error message
 * @param {Object[]} callback.response array of indexes.
 * @param {String} callback.response.indexName The name of the index.
 * @param {String} callback.response.schemaName The schema for the index.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function FetchIndex(options, callback) {
    CommandBase.call(this, 'RpbYokozunaIndexGetReq' , 'RpbYokozunaIndexGetResp', callback);
    this.validateOptions(options, schema);
}

inherits(FetchIndex, CommandBase);

FetchIndex.prototype.constructPbRequest = function() {

    var protobuf = this.getPbReqBuilder();

    if (this.options.indexName) {
        protobuf.setName(new Buffer(this.options.indexName));
    }
    return protobuf;

};

FetchIndex.prototype.onSuccess = function(rpbYokozunaIndexGetResp) {

    // If there's no indexes, Riak replies with an empty message
    var indexesToReturn;
    if (rpbYokozunaIndexGetResp) {
        indexesToReturn = new Array(rpbYokozunaIndexGetResp.index.length);
        for (var i = 0; i < rpbYokozunaIndexGetResp.index.length; i++) {
            var schema = rpbYokozunaIndexGetResp.index[i].schema;
            indexesToReturn[i] = { indexName: rpbYokozunaIndexGetResp.index[i].name.toString('utf8'),
                                   schemaName: schema ? schema.toString('utf8') : null };
            if (rpbYokozunaIndexGetResp.index[i].n_val !== null) {
                indexesToReturn[i].nVal = rpbYokozunaIndexGetResp.index[i].n_val;
            }
        }
    } else {
        indexesToReturn = [];
    }
    this._callback(null, indexesToReturn);
    return true;
};

var schema = Joi.object().keys({
    indexName: Joi.string().default(null).optional()
});

/**
 * A builder for constructing FetchIndex instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a FetchIndex directly, this builder may be used.
 *
 *     var fetch = FetchIndex.Builder()
 *                  .withIndexName('index_name')
 *                  .withCallback(callback)
 *                  .build();
 *
 * @class FetchIndex.Builder
 * @constructor
 */
function Builder(){}

Builder.prototype = {

    /**
     * The name of the index to fetch.
     * If one is not supplied, all indexes are returned.
     * @method withIndexName
     * @param {String} indexName the name of the index
     * @chainable
     */
    withIndexName : function(indexName) {
        this.indexName = indexName;
        return this;
    },
    /**
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback the callback to be executed when the operation completes.
     * @param {String} callback.err error message
     * @param {Object[]} callback.response array of indexes.
     * @param {String} callback.response.indexName The name of the index.
     * @param {String} callback.response.schemaName The schema for the index.
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
    /**
     * Construct a FetchIndex instance.
     * @method build
     * @return {FetchIndex}
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new FetchIndex(this, cb);
    }
};

module.exports = FetchIndex;
module.exports.Builder = Builder;
