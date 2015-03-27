/*
 * Copyright 2014 Basho Technologies, Inc.
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
 * Provides the FetchValue class, its builder, and its response.
 * @module ListKeys
 */

/**
 * Command used to list keys in a bucket.
 * 
 * Note that this is a __very__ expensive operations and not recommended for production use.
 * 
 * As a convenience, a builder class is provided;
 * 
 *     var ListKeys = require('lib/commands/kv/listkeys');
 *     var listKeys = new ListKeys.Builder()
 *                  .withBucketType('myBucketType')
 *                  .withBucket('myBucket')
 *                  .withCallback(myCallback)
 *                  .build();
 * 
 * 
 * @class ListKeys
 * @constructor
 * @param {Object} options the options for this command
 * @param {String} [options.bucketType=default] the bucket type in riak.
 * @param {String} options.bucket the bucket in riak.
 * @param {Function} options.callback the callback to be executed when the operation completes.
 * @param {String} options.callback.err An error message
 * @param {String[]} options.callback.response the keys returned from riak
 * @param {Boolean} options.callback.done if you have received all the keys
 * @param {Boolean} [stream=true] whether to stream or accumulate the result before calling callback
 * @param {Number} [options.timeout] set a timeout for this operation.
 * @extends CommandBase
 */
function ListKeys(options) {
    CommandBase.call(this, 'RpbListKeysReq', 'RpbListKeysResp');
    
    var self = this;
    Joi.validate(options, schema, function(err, options) {
       
        if (err) {
            throw err;
        }
    
        self.options = options;
    });
    
    if (!this.options.stream) {
        this.keys = [];
    }
    
    this.remainingTries = 1;
}

inherits(ListKeys, CommandBase);

ListKeys.prototype.constructPbRequest = function() {
  
    var protobuf = this.getPbReqBuilder();
    
    protobuf.setBucket(new Buffer(this.options.bucket));
    protobuf.setType(new Buffer(this.options.bucketType));
    protobuf.setTimeout(this.options.timeout);
    
    return protobuf;
    
};

ListKeys.prototype.onSuccess = function(rpbListKeysResp) {
    
    var keysToSend = new Array(rpbListKeysResp.keys.length);
    if (rpbListKeysResp.keys.length) {
            for (var i = 0; i < rpbListKeysResp.keys.length; i++) {
            keysToSend[i] = rpbListKeysResp.keys[i].toString('utf8');
        }
    }
        
    if (this.options.stream) {
        this.options.callback(null, keysToSend, rpbListKeysResp.done);
    } else {
        Array.prototype.push.apply(this.keys, keysToSend);
        if (rpbListKeysResp.done) {
            this.options.callback(null, this.keys, rpbListKeysResp.done);
        }
    }
    
    return rpbListKeysResp.done;
    
};

ListKeys.prototype.onRiakError = function(rpbErrorResp) {
    this.onError(rpbErrorResp.getErrmsg().toString('utf8'));
};
    
    
ListKeys.prototype.onError = function(msg) {
    this.options.callback(msg, null);
};

var schema = Joi.object().keys({
   bucket: Joi.string().required(),
   bucketType: Joi.string().default('default'),
   callback : Joi.func().required(),
   stream : Joi.boolean().default(true),
   timeout: Joi.number().default(null)
});

/**
 * A builder for constructing ListKeys instances.
 * Rather than having to manually construct the __options__ and instantiating
 * a ListKeys directly, this builder may be used.
 * 
 *     var ListKeys = require('lib/commands/kv/listkeys');
 *     var listKeys = new ListKeys.Builder()
 *                  .withBucketType('myBucketType')
 *                  .withBucket('myBucket')
 *                  .withCallback(myCallback)
 *                  .build();
 *       
 * @namespace ListKeys
 * @class Builder
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
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback - the callback to execute
     * @param {String} callback.err An error message
     * @param {String[]} callback.response - a list of keys. 
     * @param {Boolean} callback.done - listing is complete.
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
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
     * Construct a ListKeys command.
     * @method build
     * @return {ListKeys} 
     */
    build : function() {
        return new ListKeys(this);
    }
    
};

module.exports = ListKeys;
module.exports.Builder = Builder;
