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
var RpbYokozunaIndex = require('../../protobuf/riakprotobuf').getProtoFor('RpbYokozunaIndex');

/**
 * Provides the (Yokozuna) StoreIndex command.
 * @module StoreIndex
 */

/**
 * Command used to store a Yokozuna index.
 * 
 * As a convenience, a builder class is provided:
 * 
 *     var StoreIndex = require(./lib/commands/yokozuna/storeindex');
 *     var store = StoreIndex.Builder()
 *                  .withIndexName('index_name')
 *                  .withSchemaName('my_schema')
 *                  .withCallback(callback)
 *                  .build();
 *                  
 * 
 * @class StoreIndex
 * @constructor
 * @param {options} the options for this command
 * @param {String} options.indexName the name of the index. 
 * @param (String) [options.schemaName=_yz_default] the name of the schema of this index. 
 * @param {Function} options.callback the callback to be executed when the operation completes.
 * @param {String} options.callback.err error message
 * @param {Boolean} options.callback.response the operation either succeeds or errors. This will be true.
 * @extends CommandBase
 */
function StoreIndex(options) {
    
    CommandBase.call(this, 'RpbYokozunaIndexPutReq' , 'RpbPutResp');
    var self = this;
    Joi.validate(options, schema, function(err, options) {
       
        if (err) {
            throw err;
        }
    
        self.options = options;
    });
    
    this.remainingTries = 1;

    
}

inherits(StoreIndex, CommandBase);

StoreIndex.prototype.constructPbRequest = function() {
  
    var protobuf = this.getPbReqBuilder();
    
    var pbIndex = new RpbYokozunaIndex();
    pbIndex.setName(new Buffer(this.options.indexName));
    if (this.options.schemaName) {
        pbIndex.setSchema(new Buffer(this.options.schemaName));
    }
    protobuf.index = pbIndex;
    return protobuf;
    
};

StoreIndex.prototype.onSuccess = function(RpbPutResp) {
    
    // RpbPutResp is simply null (no body)
    
    this.options.callback(null, true);
    return true;
};


StoreIndex.prototype.onRiakError = function(rpbErrorResp) {
    this.onError(rpbErrorResp.getErrmsg().toString('utf8'));
};
    
    
StoreIndex.prototype.onError = function(msg) {
    this.options.callback(msg, null);
};

var schema = Joi.object().keys({
    indexName: Joi.string().required(),
    schemaName: Joi.string().default(null).optional(),
    callback: Joi.func().required()
});

/**
 * A builder for constructing StoreIndex instances.
 * * Rather than having to manually construct the __options__ and instantiating
 * a StoreIndex directly, this builder may be used.
 * 
 *     var StoreIndex = require(./lib/commands/yokozuna/storeindex');
 *     var store = StoreIndex.Builder()
 *                  .withIndexName('index_name')
 *                  .withSchemaName('my_schema')
 *                  .withCallback(callback)
 *                  .build();
 *       
 * @namespace StoreIndex
 * @class Builder
 * @constructor
 */
function Builder(){}

Builder.prototype = {
    
    /**
     * The name of the index to store.
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
     * @return {StoreIndex}
     */
    build : function() {
        return new StoreIndex(this);
    }
};

module.exports = StoreIndex;
module.exports.Builder = Builder;