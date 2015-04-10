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
 * Provides the (Yokozuna) FetchSchema command.
 * @module YZ
 */


/**
 * Command used to fetch a Yokozuna schema.
 * 
 * As a convenience, a builder class is provided:
 * 
 *     var fetch = FetchSchema.Builder()
 *                  .withSchemaName('schema_name')
 *                  .withCallback(callback)
 *                  .build();
 *                  
 * See {{#crossLink "FetchSchema.Builder"}}FetchSchema.Builder{{/crossLink}}
 * 
 * @class FetchSchema
 * @constructor
 * @param {Object} options The options for this command
 * @param {String} options.schemaName The name of the schema to fetch.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response The response from Riak.
 * @param {String} callback.response.name The schema name.
 * @param {String} callback.response.content The schema XML.
 * @extends CommandBase
 */
function FetchSchema(options, callback) {
    
    CommandBase.call(this, 'RpbYokozunaSchemaGetReq' , 'RpbYokozunaSchemaGetResp', callback);
    var self = this;
    Joi.validate(options, schema, function(err, options) {
       
        if (err) {
            throw err;
        }
    
        self.options = options;
    });
    
    this.remainingTries = 1;

    
}

inherits(FetchSchema, CommandBase);

FetchSchema.prototype.constructPbRequest = function() {
  
    var protobuf = this.getPbReqBuilder();
    
    protobuf.setName(new Buffer(this.options.schemaName));
    
    return protobuf;
    
};

FetchSchema.prototype.onSuccess = function(rpbYokozunaSchemaGetResp) {
    
    var pbSchema = rpbYokozunaSchemaGetResp.schema;
    var schema = { name: pbSchema.name.toString('utf8') };
    if (pbSchema.content) {
        schema.content = pbSchema.content.toString('utf8');
    }
    
    
    this._callback(null, schema);
    return true;
};

var schema = Joi.object().keys({
    schemaName: Joi.string().default(null).optional()
});

/**
 * A builder for constructing FetchSchema instances.
 * 
 * Rather than having to manually construct the __options__ and instantiating
 * a FetchSchema directly, this builder may be used.
 * 
 *     var fetch = FetchSchema.Builder()
 *                  .withSchemaName('schema_name')
 *                  .withCallback(callback)
 *                  .build();
 *       
 * @class FetchSchema.Builder
 * @constructor
 */
function Builder(){}

Builder.prototype = {
    
    /**
     * The name of the schema to fetch.
     * @method withSchemaName
     * @param {String} schemaName the name of the schema
     * @chainable
     */
    withSchemaName : function(schemaName) {
        this.schemaName = schemaName;
        return this;
    },
    /**
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback The callback to be executed when the operation completes.
     * @param {String} callback.err An error message. Will be null if no error.
     * @param {Object} callback.response The response from Riak.
     * @param {String} callback.response.name The schema name.
     * @param {String} callback.response.content The schema XML.
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
    /**
     * Construct a FetchSchema instance.
     * @method build
     * @return {FetchSchema}
     */
    build : function() {
        return new FetchSchema(this, this.callback);
    }
};

module.exports = FetchSchema;
module.exports.Builder = Builder;
