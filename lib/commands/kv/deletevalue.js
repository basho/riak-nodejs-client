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
 * Provides the DeleteValue class and its builder
 * @module DeleteValue
 */


/**
 * Command used to delete a value from Riak.
 * 
 * As a convenience, a builder class is provided:
 * 
 *      var deleteValue = new DeleteValue.Builder()
 *          .withBucket('myBucket')
 *          .withKey('myKey')
 *          .withVClock(vclock)
 *          .withCallback(callback)
 *          .build();
 * 
 * @class DeleteValue
 * @constructor
 * @param {Object} options the options for this command.
 * @param {String} [options.bucketType] the bucket type in riak.
 * @param {String} options.bucket the bucket in riak.
 * @param {String} options.key the key for the object you want to delete.
 * @param {Buffer} [options.vclock] the vector clock to use.
 * @param {Number} [options.timeout] set a timeout for this operation.
 * @param {Number} [options.r] the R value to use.
 * @param {Number} [options.pr] the PR value to use.
 * @param {Number} [options.w] the W value to use.
 * @param {Number} [options.dw] the DW value to use.
 * @param {Number} [options.pw] the PW value to use.
 * @param {Number} [options.rw] the RW value to use.
 * @param {Function} callback the callback to be executed when the operation completes.
 * @param {String} callback.err An error message
 * @param {Boolean} callback.response the response from Riak
 * @extends CommandBase
 */
function DeleteValue(options, callback) {
    CommandBase.call(this, 'RpbDelReq', 'RpbDelResp', callback);
    var self = this;
    Joi.validate(options, schema, function(err, options) {
       
        if (err) {
            throw err;
        }
    
        self.options = options;
    
    });
    
    this.remainingRetries = 1;
}



inherits(DeleteValue, CommandBase);

DeleteValue.prototype.constructPbRequest = function() {
    
    var protobuf = this.getPbReqBuilder();
    
    protobuf.setBucket(new Buffer(this.options.bucket));
    protobuf.setType(new Buffer(this.options.bucketType));
    protobuf.setKey(new Buffer(this.options.key));
    
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
    
    if (this.options.hasOwnProperty('r')) {
        protobuf.setR(this.options.r);
    }
    if (this.options.hasOwnProperty('pr')) {
        protobuf.setPr(this.options.pr);
    }
    
    if (this.options.hasOwnProperty('rw')) {
        protobuf.setRw(this.options.rw);
    }
    
    if (this.options.hasOwnProperty('timeout')) {
        protobuf.setTimeout(this.options.timeout);
    }
    
    return protobuf;
    
};

DeleteValue.prototype.onSuccess = function(rpbGetResp) {

    // There is no body to a RpbDelResp. RpbDelReq either 
    // succeeds or returns an error. rpbDelResp will always be null
    this._callback(null, true);
    return true;

};

var schema = Joi.object().keys({
    bucket: Joi.string().required(),
    bucketType: Joi.string().default('default'),
    key: Joi.string().required(),
    r: Joi.number().optional(),
    pr: Joi.number().optional(),
    w: Joi.number().optional(),
    dw: Joi.number().optional(),
    pw: Joi.number().optional(),
    rw: Joi.number().optional(),
    timeout: Joi.number().default(null),
    callback: Joi.func().strip().optional(),
    vclock: Joi.binary().optional()
});

/**
 * A builder for constructing DeleteValue instances.
 * * Rather than having to manually construct the __options__ and instantiating
 * a DeleteValue directly, this builder may be used.
 * 
 *      var deleteValue = new DeleteValue.Builder()
 *          .withBucket('myBucket')
 *          .withKey('myKey')
 *          .withVClock(vclock)
 *          .withCallback(callback)
 *          .build();
 *       
 * @namespace DeleteValue
 * @class Builder
 * @constructor
 */
function Builder() {
    
}

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
     * Set the key.
     * @method withKey
     * @param {String} key the key in riak.
     * @chainable
     */
    withKey : function(key) {
        this.key = key;
        return this;
    },
    /**
     * Set the R value.
     * If not asSet the bucket default is used.
     * @method withR
     * @param {Number} r the R value.
     * @chainable
     */
    withR : function(r) {
        this.r = r;
        return this;
    },
    /**
    * Set the PR value.
    * If not asSet the bucket default is used.
    * @method withPr
    * @param {Number} pr the PR value.
    * @chainable
    */
    withPr : function(pr) {
        this.pr = pr;
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
     * Set the RW value.
     * Quorum for both operations (get and put) involved in deleting an object .
     * @method withRw
     * @param {number} rw the RW value.
     * @chainable
     */
    withRw : function(rw) {
        this.rw = rw;
        return this;
    },
    /**
     * Set the vector clock.
     * If not set siblings may be created depending on bucket properties.
     * @method withVClock
     * @param {Buffer} vclock a vector clock returned from a previous fetch
     */
    withVClock : function(vclock) {
        this.vclock = vclock;
        return this;
    },/**
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
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback - the callback to execute
     * @param {String} callback.err An error message
     * @param {Boolean} callback.response - the response from Riak
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
    /**
     * Construct a DeleteValue command.
     * @method build
     * @return {DeleteValue} an instance of DeleteValue
     */
    build : function() {
        return new DeleteValue(this, this.callback);
    }
};

module.exports = DeleteValue;
module.exports.Builder = Builder;
