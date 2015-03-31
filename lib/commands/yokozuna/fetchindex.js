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
 * Provides the (Yokozuna) FetchIndex command.
 * @module FetchIndex
 */

/**
 * Command used to fetch a Yokozuna index or all indexes.
 * 
 * As a convenience, a builder class is provided:
 * 
 *     var fetch = FetchIndex.Builder()
 *                  .withIndexName('index_name')
 *                  .withCallback(callback)
 *                  .build();
 *                  
 * 
 * @class FetchIndex
 * @constructor
 * @param {options} the options for this command
 * @param {String} [options.indexName] the name of a specific index to fetch. If not supplied, all are returned.
 * @param {Function} callback the callback to be executed when the operation completes.
 * @param {String} callback.err error message
 * @param {Object[]} callback.response array of indexes.
 * @extends CommandBase
 */
function FetchIndex(options, callback) {
    
    CommandBase.call(this, 'RpbYokozunaIndexGetReq' , 'RpbYokozunaIndexGetResp', callback);
    var self = this;
    Joi.validate(options, schema, function(err, options) {
       
        if (err) {
            throw err;
        }
    
        self.options = options;
    });
    
    this.remainingTries = 1;

    
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
    indexName: Joi.string().default(null).optional(),
    callback: Joi.func().strip().optional()
});

/**
 * A builder for constructing FetchIndex instances.
 * * Rather than having to manually construct the __options__ and instantiating
 * a FetchIndex directly, this builder may be used.
 * 
 *     var fetch = FetchIndex.Builder()
 *                  .withIndexName('index_name')
 *                  .withCallback(callback)
 *                  .build();
 *       
 * @namespace FetchIndex
 * @class Builder
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
     * @param {Function} callback - the callback to execute
     * @param {String} callback.err An error message
     * @param {Object[]} callback.response - array of indexes returned from riak
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
        return new FetchIndex(this, this.callback);
    }
};

module.exports = FetchIndex;
module.exports.Builder = Builder;
