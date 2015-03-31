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
 * Provides the ListBuckets command and its builder
 * @module ListBuckets
 */

/**
 * Command used to list buckets in a bucket type.
 * 
 * As a convenience, a builder class is provided;
 * 
 *     var listBuckets = new ListBuckets.Builder()
 *                  .withBucketType('myBucketType')
 *                  .withCallback(myCallback)
 *                  .build();
 * 
 * 
 * @class ListBuckets
 * @constructor
 * @param {Object} options the options for this command
 * @param {String} [options.bucketType=default] the bucket type in riak.
 * @param {Boolean} [stream=true] whether to stream or accumulate the result before calling callback
 * @param {Number} [options.timeout] set a timeout for this operation.
 * @param {Function} callback the callback to be executed when the operation completes.
 * @param {String} callback.err An error message
 * @param {String[]} callback.response the buckets returned from riak
 * @param {Boolean} callback.done if you have received all the buckets

 * @extends CommandBase
 */
function ListBuckets(options, callback) {
    CommandBase.call(this, 'RpbListBucketsReq', 'RpbListBucketsResp', callback);
    
    var self = this;
    Joi.validate(options, schema, function(err, options) {
       
        if (err) {
            throw err;
        }
    
        self.options = options;
    });
    
    if (!this.options.stream) {
        this.buckets = [];
    }
    
    this.remainingTries = 1;
}

inherits(ListBuckets, CommandBase);

ListBuckets.prototype.constructPbRequest = function() {
  
    var protobuf = this.getPbReqBuilder();
    
    protobuf.setType(new Buffer(this.options.bucketType));
    protobuf.setTimeout(this.options.timeout);
    // We always stream from Riak 'cause it's better
    protobuf.stream = true;
    
    return protobuf;
    
};

ListBuckets.prototype.onSuccess = function(rpbListBucketsResp) {
    
    var bucketsToSend = new Array(rpbListBucketsResp.buckets.length);
    if (rpbListBucketsResp.buckets.length) {
            for (var i = 0; i < rpbListBucketsResp.buckets.length; i++) {
            bucketsToSend[i] = rpbListBucketsResp.buckets[i].toString('utf8');
        }
    }
        
    if (this.options.stream) {
        this._callback(null, { buckets: bucketsToSend, done: rpbListBucketsResp.done });
    } else {
        Array.prototype.push.apply(this.buckets, bucketsToSend);
        if (rpbListBucketsResp.done) {
            this._callback(null, { buckets: this.buckets, done: rpbListBucketsResp.done});
        }
    }
    
    return rpbListBucketsResp.done;
    
};

var schema = Joi.object().keys({
   bucketType: Joi.string().default('default'),
   callback : Joi.func().strip().optional(),
   stream : Joi.boolean().default(true),
   timeout: Joi.number().default(null)
});

/**
 * A builder for constructing ListBuckets instances.
 * * Rather than having to manually construct the __options__ and instantiating
 * a ListBuckets directly, this builder may be used.
 * 
 *      var listBuckets = new ListBuckets.Builder()
 *                          .withBucketType('myBucketType')
 *                          .withCallback(myCallback)
 *                          .build();
 *       
 * @namespace ListBuckets
 * @class Builder
 * @constructor
 */
function Builder(){}

Builder.prototype = {
    
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
     * @param {String[]} callback.response - a list of buckets. 
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
     * Construct a ListBuckets command.
     * @method build
     * @return {ListKeys} 
     */
    build : function() {
        return new ListBuckets(this, this.callback);
    }
};

module.exports = ListBuckets;
module.exports.Builder = Builder;
