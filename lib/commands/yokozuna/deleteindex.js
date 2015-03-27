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
 * Provides the (Yokozuna) DeleteIndex command and its builder.
 * @module DeleteIndex
 */

/**
 * Command used to Delete a Yokozuna index.
 * 
 * As a convenience, a builder class is provided:
 * 
 *     var del = DeleteIndex.Builder()
 *                  .withIndexName('index_name')
 *                  .withCallback(callback)
 *                  .build();
 *                  
 * 
 * @class DeleteIndex
 * @constructor
 * @param {options} the options for this command
 * @param {String} [options.indexName] the name of the index to delete
 * @param {Function} options.callback the callback to be executed when the operation completes.
 * @param {String} options.callback.err error message
 * @param {Boolean} options.callback.response operation either succeeds or returns error. This will be true.
 * @extends CommandBase
 */
function DeleteIndex(options) {
    
    CommandBase.call(this, 'RpbYokozunaIndexDeleteReq' , 'RpbDelResp');
    var self = this;
    Joi.validate(options, schema, function(err, options) {
       
        if (err) {
            throw err;
        }
    
        self.options = options;
    });
    
    this.remainingTries = 1;

    
}

inherits(DeleteIndex, CommandBase);

DeleteIndex.prototype.constructPbRequest = function() {
  
    var protobuf = this.getPbReqBuilder();
    
    protobuf.setName(new Buffer(this.options.indexName));
    
    return protobuf;
    
};

DeleteIndex.prototype.onSuccess = function(RpbDelResp) {
    
    // RpbDelResp is simply null (no body)
    
    this.options.callback(null, true);
    return true;
};

DeleteIndex.prototype.onRiakError = function(rpbErrorResp) {
    this.onError(rpbErrorResp.getErrmsg().toString('utf8'));
};
    
    
DeleteIndex.prototype.onError = function(msg) {
    this.options.callback(msg, null);
};

var schema = Joi.object().keys({
    indexName: Joi.string().required(),
    callback: Joi.func().required()
});

/**
 * A builder for constructing DeleteIndex instances.
 * * Rather than having to manually construct the __options__ and instantiating
 * a DeleteIndex directly, this builder may be used.
 * 
 *     var del = DeleteIndex.Builder()
 *                  .withIndexName('index_name')
 *                  .withCallback(callback)
 *                  .build();
 *       
 * @namespace DeleteIndex
 * @class Builder
 * @constructor
 */
function Builder(){}

Builder.prototype = {
    
    /**
     * The name of the index to delete.
     * If one is not supplied, all indexes are returned.
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
     * @param {Function} callback the callback to execute
     * @param {String} callback.err An error message
     * @param {Boolean} callback.response will always be true.
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
    /**
     * Construct a DeleteIndex instance.
     * @return {DeleteIndex}
     */
    build : function() {
        return new DeleteIndex(this);
    }
};

module.exports = DeleteIndex;
module.exports.Builder = Builder;
