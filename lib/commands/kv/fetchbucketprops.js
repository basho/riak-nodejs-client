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
 * Provides the FetchBucketProps class, its builder, and its response.
 * @module KV
 */


/**
 * Command used to fetch a bucket's properties from Riak.
 * 
 * As a convenience, a builder class is provided:
 * 
 *     var fetch = new FetchBucketProps.Builder()
 *         .withBucketType('my_type')
 *         .withBucket('myBucket')
 *         .withCallback(myCallback)
 *         .build();
 *         
 * See {{#crossLink "FetchBucketProps.Builder"}}FetchBucketProps.Builder{{/crossLink}}
 * @class FetchBucketProps
 * @constructor
 * @param {Object} options The options for this command
 * @param {String} [options.bucketType=default] The bucket type in riak.
 * @param {String} options.bucket The bucket in riak.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will ne null if no error.
 * @param {Object} callback.response The response from Riak. This is an oject with all the bucket properties.
 * @extends CommandBase
 */
function FetchBucketProps(options, callback) {
    
    CommandBase.call(this, 'RpbGetBucketReq','RpbGetBucketResp', callback);
    var self = this;
    Joi.validate(options, schema, function(err, options) {
       
        if (err) {
            throw err;
        }
    
        self.options = options;
    });
        
    this.remainingTries = 1;

}

inherits(FetchBucketProps, CommandBase);

FetchBucketProps.prototype.constructPbRequest = function() {
  
    var protobuf = this.getPbReqBuilder();
    
    protobuf.setType(new Buffer(this.options.bucketType));
    protobuf.setBucket(new Buffer(this.options.bucket));
    
    return protobuf;
    
};

FetchBucketProps.prototype.onSuccess = function(rpbGetBucketResp) {
    
    var rpbBucketProps = rpbGetBucketResp.getProps();
    
    var bucketProps  = {
        
        nVal : rpbBucketProps.getNVal(),
        allowMult : rpbBucketProps.getAllowMult(),
        lastWriteWins: rpbBucketProps.getLastWriteWins(),
        hasPrecommit : rpbBucketProps.getHasPrecommit(),
        hasPostcommit: rpbBucketProps.getHasPostcommit(),
        oldVClock : rpbBucketProps.getOldVclock(),
        youngVClock: rpbBucketProps.getYoungVclock(),
        bigVClock : rpbBucketProps.getBigVclock(),
        smallVClock: rpbBucketProps.getSmallVclock(),
        pr: rpbBucketProps.getPr(),
        r: rpbBucketProps.getR(),
        w: rpbBucketProps.getW(),
        pw: rpbBucketProps.getPw(),
        dw: rpbBucketProps.getDw(),
        rw: rpbBucketProps.getRw(),
        basicQuorum: rpbBucketProps.getBasicQuorum(),
        notFoundOk: rpbBucketProps.getNotfoundOk(),
        search: rpbBucketProps.getSearch(),
        consistent: rpbBucketProps.getConsistent(),
        repl: rpbBucketProps.getRepl()
    };
    
    var backend;
    if ((backend = rpbBucketProps.getBackend()) !== null) {
        bucketProps.backend = backend.toString('utf8');
    }
    
    var searchIndex;
    if ((searchIndex = rpbBucketProps.getSearchIndex()) !== null) {
        bucketProps.searchIndex = searchIndex.toString('utf8');
    }
    
    var dataType;
    if ((dataType = rpbBucketProps.getDatatype()) !== null) {
        bucketProps.dataType = dataType.toString('utf8');
    }
    
    if (rpbBucketProps.getHasPrecommit()) {
        bucketProps.precommit = this._parseHooks(rpbBucketProps.getPrecommit());
    } else {
        bucketProps.precommit = [];
    }
    
    if (rpbBucketProps.getHasPostcommit()) {
        bucketProps.postcommit = this._parseHooks(rpbBucketProps.getPostcommit());
    } else {
        bucketProps.postcommit = [];
    }
    
    var chashKeyFun;
    if ((chashKeyFun = rpbBucketProps.getChashKeyfun()) !== null) {
        bucketProps.chashKeyfun = { mod: chashKeyFun.module.toString('utf8'),
            fun: chashKeyFun.function.toString('utf8') };
    }
    
    var linkfun;
    if ((linkfun = rpbBucketProps.getLinkfun()) !== null) {
        bucketProps.linkFun = { mod: linkfun.module.toString('utf8'),
            fun: linkfun.function.toString('utf8') };
    }
    
    this._callback(null, bucketProps);
    return true;
    
};

FetchBucketProps.prototype._parseHooks = function(rpbCommitHooks) {
    var hooks = new Array(rpbCommitHooks.length);
    for (var i = 0; i < rpbCommitHooks.length; i++) {
        if (rpbCommitHooks[i].name) {
            hooks[i] = { name: rpbCommitHooks[i].name.toString('utf8') };
        } else {
            hooks[i] = { mod: rpbCommitHooks[i].modfun.module.toString('utf8'), 
                fun: rpbCommitHooks[i].modfun.function.toString('utf8') };
        }
    }
    return hooks;
    
};

var schema = Joi.object().keys({
   bucket: Joi.string().required(),
   bucketType: Joi.string().default('default'),
   callback: Joi.func().strip().optional()
});

/**
 * A builder for constructing GetBucketProps instances
 * 
 * Rather than having to manually construct the __options__ and instantiating
 * a FetchBucketProps directly, this builder may be used.
 *
 *     var fetch = new FetchBucketProps.Builder()
 *         .withBucketType('my_type')
 *         .withBucket('myBucket')
 *         .withCallback(myCallback)
 *         .build();
 *
 * @class FetchBucketProps.Builder
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
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback The callback to be executed when the operation completes.
     * @param {String} callback.err An error message. Will ne null if no error.
     * @param {Object} callback.response The response from Riak. This is an oject with all the bucket properties.
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
    /**
     * Construct a FetchBucketProps instance.
     * @method build
     * @return {FetchBucketProps}
     */
    build : function() {
        return new FetchBucketProps(this, this.callback);
    }
};

module.exports = FetchBucketProps;
module.exports.Builder = Builder;
