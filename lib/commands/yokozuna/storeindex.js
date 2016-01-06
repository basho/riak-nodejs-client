'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');
var RpbYokozunaIndex = require('../../protobuf/riakprotobuf').getProtoFor('RpbYokozunaIndex');

/**
 * Provides the (Yokozuna) StoreIndex command.
 * @module YZ
 */

/**
 * Command used to store a Yokozuna index.
 *
 * As a convenience, a builder class is provided:
 *
 *     var store = StoreIndex.Builder()
 *                  .withIndexName('index_name')
 *                  .withSchemaName('my_schema')
 *                  .withCallback(callback)
 *                  .build();
 *
 * See {{#crossLink "StoreIndex.Builder"}}StoreIndex.Builder{{/crossLink}}
 *
 * @class StoreIndex
 * @constructor
 * @param {Object} options The options for this command
 * @param {String} options.indexName The name of the index.
 * @param {String} [options.schemaName=_yz_default] The name of the schema for this index.
 * @param {Number} [options.timeout] Set a timeout for this operation.
 * @param {Number} [options.nVal] The number of replicas.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Boolean} callback.response The operation either succeeds or errors. This will be true.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function StoreIndex(options, callback) {
    CommandBase.call(this, 'RpbYokozunaIndexPutReq' , 'RpbPutResp', callback);
    this.validateOptions(options, schema);
}

inherits(StoreIndex, CommandBase);

StoreIndex.prototype.constructPbRequest = function() {

    var protobuf = this.getPbReqBuilder();

    var pbIndex = new RpbYokozunaIndex();

    pbIndex.setName(new Buffer(this.options.indexName));
    pbIndex.setNVal(this.options.nVal > 0 ? this.options.nVal : null );

    if (this.options.schemaName) {
        pbIndex.setSchema(new Buffer(this.options.schemaName));
    }

    protobuf.index = pbIndex;

    if (this.options.timeout) {
        protobuf.setTimeout(this.options.timeout);
    }

    return protobuf;

};

StoreIndex.prototype.onSuccess = function(RpbPutResp) {
    // RpbPutResp is simply null (no body)
    this._callback(null, true);
    return true;
};

var schema = Joi.object().keys({
    indexName: Joi.string().required(),
    schemaName: Joi.string().default(null).optional(),
    timeout: Joi.number().default(null).optional(),
    nVal: Joi.number().optional()
});

/**
 * A builder for constructing StoreIndex instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a StoreIndex directly, this builder may be used.
 *
 *     var store = StoreIndex.Builder()
 *                  .withIndexName('index_name')
 *                  .withSchemaName('my_schema')
 *                  .withCallback(callback)
 *                  .build();
 *
 * @class StoreIndex.Builder
 * @constructor
 */
function Builder(){}

Builder.prototype = {

    /**
     * The name of the index to store.
     * @method withIndexName
     * @param {String} indexName the name of the index
     * @chainable
     */
    withIndexName : function(indexName) {
        this.indexName = indexName;
        return this;
    },
    /**
     * The name of the schema to use with this index.
     * If not provided the default '_yz_default' will be used.
     * @method withSchemaName
     * @param {String} schemaName the name of the schema to use.
     * @chainable
     */
    withSchemaName : function(schemaName) {
        this.schemaName = schemaName;
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
    * Set the nVal.
    * @method withNVal
    * @param {Number} nVal the number of replicas.
    * @chainable
    */
    withNVal : function(nVal) {
        this.nVal = nVal;
        return this;
    },
    /**
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback the callback to execute
     * @param {String} callback.err An error message
     * @param {Boolean} callback.response operation either succeeds or errors. This will be true.
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
    /**
     * Construct a StoreIndex instance.
     * @method build
     * @return {StoreIndex}
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new StoreIndex(this, cb);
    }
};

module.exports = StoreIndex;
module.exports.Builder = Builder;
