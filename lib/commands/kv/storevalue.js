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

var RiakMeta = require('./riakmeta');
var KvResponseBase = require('./kvresponsebase');
var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');

/**
 * Provides the StoreValue class, its builder, and its response.
 * @module StoreValue
 */

/**
 * Command used to store data in Riak.
 * 
 * As a convenience, a builder class is provided:
 * 
 *      var StoreValue = require('./lib/commands/storevalue');
 *      var storeValue = new StoreValue.Builder()
 *          .withBucket('myBucket')
 *          .withKey('myKey')
 *          .withContent(myObj, myMeta);
 *          .build();
 * 
 * @class StoreValue
 * @constructor
 * @param {Object} options the options for this command.
 * @param {String} [options.bucketType] the bucket type in riak.
 * @param {String} options.bucket the bucket in riak.
 * @param {String} [options.key] the key for the object you want to store.
 * @param {Function} options.callback the callback to be executed when the operation completes.
 * @param {String} options.callback.err An error message
 * @param {StoreValue.Response} options.callback.response the response from Riak
 * @param {String|Buffer|Object} options.value the value to store in Riak
 * @param {RiakMeta} [options.meta] the metadata for the value to store in riak
 * @param {Number} [options.w] the W value to use.
 * @param {Number} [options.dw] the DW value to use.
 * @param {Number} [options.pw] the PW value to use.
 * @param {Boolean} [options.returnBody] return the stored object and meta (incl. siblings)
 * @param {Boolean} [options.returnHead] return the metatdata only for the stored object.
 * @param {Number} [options.timeout] set a timeout for this command.
 * @param {Boolean} [options.ifNotModified] the if_not_modified flag.
 * @param {Boolean} [options.ifNoneMatch] the if_none_match flag.
 * @extends CommandBase
 */
function StoreValue(options) {
    CommandBase.call(this, 'RpbPutReq', 'RpbPutResp');
    var self = this;
    Joi.validate(options, schema, function(err, options) {
       
        if (err) {
            throw err;
        }
    
        self.options = options;
    
        self.callback = options.callback;
        self.bucket = options.bucket;
        self.bucketType = options.bucketType;
        
        if (options.hasOwnProperty('key')) {
            self.key = options.key;
        } 
        
    });
    
    this.remainingTries = 1;
};

inherits(StoreValue, CommandBase);

StoreValue.prototype.constructPbRequest = function() {
    
    var protobuf = this.getPbReqBuilder();
    
    protobuf.setBucket(new Buffer(this.options.bucket));
    protobuf.setType(new Buffer(this.options.bucketType));
    
    if (this.options.hasOwnProperty('key')) {
        protobuf.setKey(new Buffer(this.options.key));
    } 
    
    if (this.options.hasOwnProperty('vclock')) {
        protobuf.setVclock(this.options.vclock);
    }
    
    if (this.options.hasOwnProperty('w')) {
        protobuf.setW(this.options.w);
    }
    
    if (this.options.hasOwnProperty('dw')) {
        protobuf.setDw(this.options.dw);
    }
    
    if (this.options.hasOwnProperty('pw')) {
        protobuf.setPw(this.options.pw);
    }
    
    if (this.options.hasOwnProperty('returnBody')) {
        protobuf.setReturnBody(this.options.returnBody);
    }
    
    if (this.options.hasOwnProperty('returnHead')) {
        protobuf.setReturnHead(this.options.returnHead);
    }
    
    if (this.options.hasOwnProperty('timeout')) {
        protobuf.setTimeout(this.options.timeout);
    }
    
    if (this.options.hasOwnProperty('ifNotModified')) {
        protobuf.setIfNotModified(this.options.ifNotModified);
    }
    
    if (this.options.hasOwnProperty('ifNoneMatch')) {
        protobuf.setIfNoneMatch(this.options.ifNoneMatch);
    }
    
    var meta;
    if (this.options.hasOwnProperty('meta')) {
        meta = this.options.meta;
        // meta takes precidence 
        if (meta.hasOwnProperty('vclock')) {
            protobuf.setVclock(meta.vclock);
        }
        if (meta.hasOwnProperty('bucket')) {
            protobuf.setBucket(new Buffer(meta.bucket));
        }
        if (meta.hasOwnProperty('bucketType')) {
            protobuf.setType(new Buffer(meta.bucketType));
        }
        if (meta.hasOwnProperty('key')) {
            protobuf.setKey(new Buffer(meta.key));
        }
        
    } else {
        meta = null;
    }
    
    var rpbContent = RiakMeta.populateRpbContentFromValueAndMeta(this.options.value, meta);
    
    protobuf.setContent(rpbContent);
    
    return protobuf;
    
};

StoreValue.prototype.onSuccess = function(rpbPutResp) {
    
    var responseKey;
    
    if (rpbPutResp.getKey()) {
        responseKey = rpbPutResp.getKey().toString('utf8');
    } 
    
    
    var values;
    // If returnBody or returnHead was set
    if (rpbPutResp.getContent().length > 0) {
        var pbContentArray = rpbPutResp.getContent();
        var vclock = rpbGetResp.getVclock().toBuffer();
        
        values = new Array(pbContentArray.length);

        for (var i = 0; i < pbContentArray.length; i++) {
            var riakMeta = RiakMeta.extractMetaFromRpbContent(pbContentArray[i], vclock, this.bucketType, this.bucket, responseKey ? responseKey : this.key);
            var riakValue = pbContentArray[i].getValue().toBuffer();
            values[i] = new KvResponseBase.KvValueMetaPair(riakValue, riakMeta);
        }
    } else {
        values =[];
    }
    
    this.callback(new Response(values, responseKey));
    
};

StoreValue.prototype.onRiakError = function(rpbErrorResp) {
    this.onError(rpbErrorResp.getErrmsg().toString('utf8'));
};
    
    
StoreValue.prototype.onError = function(msg) {
    this.callback(msg, null);
};

var schema = Joi.object().keys({
    bucket: Joi.string().required(),
    bucketType: Joi.string().default('default'),
    key: Joi.string().optional(),
    callback: Joi.func().required(),
    value: Joi.any().required(),
    meta: Joi.object().optional(),
    w: Joi.number().optional(),
    dw: Joi.number().optional(),
    pw: Joi.number().optional(),
    returnBody: Joi.boolean().optional(),
    returnHead: Joi.boolean().optional(),
    timeout: Joi.number().optional(),
    ifNotModified: Joi.boolean().optional(),
    ifNoneMatch: Joi.boolean().optional(),
    vclock: Joi.binary().optional()
    
});


/**
 * A builder for constructing StoreValue instances.
 * * Rather than having to manually construct the __options__ and instantiating
 * a StoreValue directly, this builder may be used.
 * 
 *      var StoreValue = require('./lib/commands/storevalue');
 *      var storeValue = new StoreValue.Builder()
 *          .withBucket('myBucket')
 *          .withKey('myKey')
 *          .withContent(myObj, myMeta);
 *          .build();
 *       
 * @namespace StoreValue
 * @class Builder
 * @constructor
 */
function Builder() {}

Builder.prototype = {
  
    
    /**
     * Set the bucket.
     * Note that this can also be provided via RiakMeta passed in via withContent() and
     * doing so will take precidence over this method.
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
     * Note that this can also be provided via RiakMeta passed in via withContent() and
     * doing so will take precidence over this method.
     * @method withBucketType
     * @param {String} bucketType the bucket type in riak
     * @chainable
     */
    withBucketType : function(bucketType) {
        this.bucketType = bucketType;
        return this;
    },
    /**
     * Set the key.
     * If not set, riak will generate and return a key.
     * Note that this can also be provided via RiakMeta passed in via withContent() and
     * doing so will take precidence over this method.
     * @method withKey
     * @param {String} key the key in riak.
     * @chainable
     */
    withKey : function(key) {
        this.key = key;
        return this;
    },
    /**
     * Set the value and its metadata to be stored in Riak.
     * If a JS object is supplied, it will be converted to JSON
     * using JSON.stringify()
     * @method withContent
     * @param {String|Buffer|Object} value the value to store in Riak
     * @param {RiakMeta} [meta] the metadata associated with this value. 
     * @chainable
     */
    withContent : function(value, meta) {
        this.value = value;
        this.meta = meta;
        return this;
    },
    /**
     * Set the vector clock.
     * Convenience method if RiakMeta is not supplied. 
     * Note that a vclock supplied via RiakMeta in withContent() will have precendence over this.
     * @method withVClock
     * @param {Buffer} vclock a vector clock returned from a previous fetch
     */
    withVClock : function(vclock) {
        this.vclock = vclock;
        return this;
    },
    /**
    * Set the W value.
    * How many replicas to write to before returning a successful response.
    * If not set the bucket default is used.
    * @method withW
    * @param {number} w the W value.
    * @chainable
    */
    withW : function(w) {
        this.w = w ;
        return this;
    },
    /**
     * Set the DW value.
     * How many replicas to commit to durable storage before returning a successful response.
     * If not set the bucket default is used.
     * @method withDw
     * @param {number} dw the DW value.
     * @chainable
     */
    withDw : function(dw) {
        this.dw = dw;
        return this;
    },
    /**
     * Set the PW value.
     * How many primary nodes must be up when the write is attempted.
     * If not set the bucket default is used.
     * @method withPw
     * @param {number} pw the PW value.
     * @chainable
     */
    withPw : function(pw) {
        this.pw = pw;
        return this;
    },
    /**
    * Return the object after storing (including any siblings).
    * @method withReturnBody
    * @param {boolean} returnBody true to return the object. 
    * @chainable
    */
    withReturnBody: function(returnBody) {
        this.returnBody = returnBody;
        return this;
    },
    /**
    * Return the metadata after storing the value.
    * 
    * Causes Riak to only return the metadata for the object. The value
    * will be asSet to null.
    * @method withReturnHead
    * @param {boolean} returnHead true to return only metadata. 
    * @chainable
    */
    withReturnHead : function(returnHead) {
        this.returnHead = returnHead;
        return this;
    },
    /**
    * Set the if_not_modified flag.
    *
    * Setting this to true means to store the value only if the 
    * supplied vclock matches the one in the database.
    *
    * Be aware there are several cases where this may not actually happen.
    * Use of this feature is discouraged.
    * @method withIfNotModified
    * @param {Boolean} ifNotModified the if_not_modified value.
    * @chainable
    */
    withIfNotModified : function(ifNotModified) {
        this.ifNotModified = ifNotModified;
        return this;
    },
    /**
    * Set the if_none_match flag.
    * 
    * Setting this to true means store the value only if this 
    * bucket/key combination are not already defined. 
    * 
    * Be aware that there are several cases where 
    * this may not actually happen. Use of this option is discouraged.
    * @method withIfNoneMatch
    * @param {boolean} ifNoneMatch the if_non-match value.
    * @chainable
    */
    withIfNoneMatch : function(ifNoneMatch) {
        this.ifNoneMatch = ifNoneMatch;
        return this;
    },
    /**
    * Set a timeout for this operation.
    * @method withTimeout
    * @param {number} timeout a timeout in milliseconds.
    * @chainable
    */
    withTimeout : function(timeout) {
        this.timeout = timeout;
        return this;
    },
    /**
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback - the callback to execute
     * @param {String} callback.err An error message
     * @param {StoreValue.Response} callback.response - the response from Riak
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
    /**
     * Construct a StoreValue instance.
     * @method build
     * @return {StoreValue} a StoreValue instance
     */
    build : function() {
        return new StoreValue(this);
    }
    
};

/**
 * The response from a StoreValue command.
 * @namespace StoreValue
 * @class Response
 * @constructor
 * @param {KvValueMetaPair[]} valueMetaPairs array of values and their meta
 * @param {String} [generatedKey] key generated by Riak
 * @extends KvResponsBase
 */
function Response(valueMetaPairs, generatedKey) {
    KvResponseBase.call(this, valueMetaPairs);
    
    this.generatedKey = generatedKey;
}

inherits(Response, KvResponseBase);

/**
 * Check to see if this response contains a key generated by Riak.
 * This is also injected into the RiakMeta.
 * @method hasGeneratedKey
 * @return {Boolean} true if a generated key exists.
 */
Response.prototype.hasGeneratedKey = function() {
    return this.generatedKey !== undefined;
};

/**
 * Get the generated key.
 * @methos getGeneratedKey
 * @return {String} the key generated by riak, or undefined if none exists.
 */
Response.prototype.getGeneratedKey = function() {
    return this.generatedKey;
};



module.exports = StoreValue;
module.exports.Builder = Builder;

