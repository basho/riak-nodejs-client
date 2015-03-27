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
var RpbYokozunaSchema = require('../../protobuf/riakprotobuf').getProtoFor('RpbYokozunaSchema');

/**
 * Provides the (Yokozuna) StoreSchema command.
 * @module StoreSchema
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
 * 
 * @class StoreSchema
 * @constructor
 * @param {options} the options for this command
 * @param {String} options.schemaName the name of the schema to store.
 * @param {String} options.schema The XML that defines this schema
 * @param {Function} options.callback the callback to be executed when the operation completes.
 * @param {String} options.callback.err error message
 * @param {Boolean} options.callback.response the schemathis operation either succeeds or errors. This will be true.
 * @extends CommandBase
 */
function StoreSchema(options) {
    
    CommandBase.call(this, 'RpbYokozunaSchemaPutReq' , 'RpbPutResp');
    var self = this;
    Joi.validate(options, schema, function(err, options) {
       
        if (err) {
            throw err;
        }
    
        self.options = options;
    });
    
    this.remainingTries = 1;

    
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
    this.options.callback(null, true);
    return true;
};

StoreSchema.prototype.onRiakError = function(rpbErrorResp) {
    this.onError(rpbErrorResp.getErrmsg().toString('utf8'));
};
    
    
StoreSchema.prototype.onError = function(msg) {
    this.options.callback(msg, null);
};

var schema = Joi.object().keys({
    schemaName: Joi.string().required(),
    schema: Joi.string().required(),
    callback: Joi.func().required()
});

/**
 * A builder for constructing StoreSchema instances.
 * * Rather than having to manually construct the __options__ and instantiating
 * a StoreSchema directly, this builder may be used.
 * 
 *     var store = StoreSchema.Builder()
 *                  .withSchemaName('schema_name')
 *                  .withSchema(mySchemaXML)
 *                  .withCallback(callback)
 *                  .build();
 *       
 * @namespace StoreSchema
 * @class Builder
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
     * @return {StoreSchema}
     */
    build : function() {
        return new StoreSchema(this);
    }
};

module.exports = StoreSchema;
module.exports.Builder = Builder;
