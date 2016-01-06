'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');
var RpbYokozunaSchema = require('../../protobuf/riakprotobuf').getProtoFor('RpbYokozunaSchema');

/**
 * Provides the (Yokozuna) StoreSchema command.
 * @module YZ
 */

/**
 * Command used to store a Yokozuna schema.
 *
 * As a convenience, a builder class is provided:
 *
 *     var store = StoreSchema.Builder()
 *                  .withSchemaName('schema_name')
 *                  .withSchema(mySchemaXML)
 *                  .withCallback(callback)
 *                  .build();
 *
 * See {{#crossLink "StoreSchema.Builder"}}StoreSchema.Builder{{/crossLink}}
 *
 * @class StoreSchema
 * @constructor
 * @param {Object} options The options for this command
 * @param {String} options.schemaName The name of the schema to store.
 * @param {String} options.schema The XML that defines this schema
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Boolean} callback.response This operation either succeeds or errors. This will be true.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function StoreSchema(options, callback) {
    CommandBase.call(this, 'RpbYokozunaSchemaPutReq' , 'RpbPutResp', callback);
    this.validateOptions(options, schema);
}

inherits(StoreSchema, CommandBase);

StoreSchema.prototype.constructPbRequest = function() {

    var protobuf = this.getPbReqBuilder();
    var pbSchema = new RpbYokozunaSchema();
    pbSchema.name = new Buffer(this.options.schemaName);
    pbSchema.content = new Buffer(this.options.schema);
    protobuf.schema = pbSchema;

    return protobuf;

};

StoreSchema.prototype.onSuccess = function(rpbPutResp) {

    // rpbPutResp will be null (no body)
    this._callback(null, true);
    return true;
};

var schema = Joi.object().keys({
    schemaName: Joi.string().required(),
    schema: Joi.string().required()
});

/**
 * A builder for constructing StoreSchema instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a StoreSchema directly, this builder may be used.
 *
 *     var store = StoreSchema.Builder()
 *                  .withSchemaName('schema_name')
 *                  .withSchema(mySchemaXML)
 *                  .withCallback(callback)
 *                  .build();
 *
 * @class StoreSchema.Builder
 * @constructor
 */
function Builder(){}

Builder.prototype = {

    /**
     * The name of the schema.
     * @method withSchemaName
     * @param {String} schemaName the name of the schema.
     * @chainable
     */
    withSchemaName : function(schemaName) {
        this.schemaName = schemaName;
        return this;
    },
    /**
     * The XML that defines this schema
     * @method withSchema
     * @param {String} schemaXML The XML that defines the schema
     * @chainable
     */
    withSchema : function(schemaXML) {
        this.schema = schemaXML;
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
     * Construct a StoreSchema instance.
     * @method build
     * @return {StoreSchema}
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new StoreSchema(this, cb);
    }
};

module.exports = StoreSchema;
module.exports.Builder = Builder;
