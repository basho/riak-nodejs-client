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
var RpbBucketProps = require('../../protobuf/riakprotobuf').getProtoFor('RpbBucketProps');
var RpbCommitHook = require('../../protobuf/riakprotobuf').getProtoFor('RpbCommitHook');
var RpbModFun = require('../../protobuf/riakprotobuf').getProtoFor('RpbModFun');
var inherits = require('util').inherits;
var Joi = require('joi');

/**
 * Provides the StoreBucketProps class, its builder, and its response.
 * @module StoreBucketProps
 */

/**
 * Command used to set the properties on a bucket in Riak.
 * 
 * As a convenience, a builder class is provided:
 * 
 *     var StoreBucketProps = require('lib/commands/kv/storebucketprops');
 *     var storeProps = new StoreBucketProps.Builder()
 *                  .withAllowMult(true)
 *                  .build();
 * 
 * @class StoreBucketProps
 * @constructor
 * @param {Object} options the properties to store
 * 
 * @extends CommandBase
 */
function StoreBucketProps(options) {
    CommandBase.call(this, 'RpbSetBucketReq', 'RpbSetBucketResp');
    
    var self = this;
    Joi.validate(options, schema, function(err, options) {
       
        if (err) {
            throw err;
        }
    
        self.options = options;
    });
    
    this.remainingTries = 1;

}

inherits(StoreBucketProps, CommandBase);

StoreBucketProps.prototype.constructPbRequest = function() {
    
    var protobuf = this.getPbReqBuilder();
    
    protobuf.setBucket(new Buffer(this.options.bucket));
    protobuf.setType(new Buffer(this.options.bucketType));
    
    var props = new RpbBucketProps();
    
    props.setNVal(this.options.nVal > 0 ? this.options.nVal : null );
    props.setOldVclock(this.options.oldVClock >= 0 ? this.options.oldVClock : null);
    props.setYoungVclock(this.options.youngVClock >= 0 ? this.options.youngVClock : null);
    props.setBigVclock(this.options.bigVClock >= 0 ? this.options.bigVClock : null);
    props.setSmallVclock(this.options.smallVClock >= 0 ? this.options.smallVClock : null);
    props.setR(this.options.r >= 0 ? this.options.r : null);
    props.setPr(this.options.pr >= 0 ? this.options.pr : null);
    props.setW(this.options.w >= 0 ? this.options.w : null);
    props.setPw(this.options.pw >= 0 ? this.options.pw : null);
    props.setDw(this.options.dw >= 0 ? this.options.dw : null);
    props.setRw(this.options.rw >= 0 ? this.options.rw : null);
    
    
    props.setBasicQuorum(typeof this.options.basicQuorum === 'boolean' ? this.options.basicQuorum : null);
    props.setNotfoundOk(typeof this.options.notFoundOk === 'boolean' ? this.options.notFoundOk : null);
    props.setSearch(typeof this.options.search === 'boolean' ? this.options.search : null);
    props.setAllowMult(typeof this.options.allowMult === 'boolean' ? this.options.allowMult : null);
    props.setLastWriteWins(typeof this.options.lastWriteWins === 'boolean' ? this.options.lastWriteWins : null);
    
    props.setBackend(this.options.backend ? new Buffer(this.options.backend) : null);
    props.setSearchIndex(this.options.searchIndex ? new Buffer(this.options.searchIndex) : null);
    
    if (this.options.precommit) {
        props.setPrecommit(this._convertHooks(this.options.precommit));
    }
    
    if (this.options.postcommit) {
        props.setPostcommit(this._convertHooks(this.options.postcommit));
    }
    
    if (this.options.chashKeyfun) {
        var modfun = new RpbModFun();
        modfun.module = new Buffer(this.options.chashKeyfun.mod);
        modfun.function = new Buffer(this.options.chashKeyfun.fun);
        props.setChashKeyfun(modfun);
    }
    
    protobuf.setProps(props);
    
    return protobuf;
    
};

StoreBucketProps.prototype._convertHooks = function(hooks) {
    var rpbHooks = new Array(hooks.length);
    for (var i = 0; i < hooks.length; i++) {
        var hook = new RpbCommitHook();
        if (this.options.precommit[i].name) {
            hook.name = new Buffer(this.options.precommit[i].name);
        } else {
            var modfun = new RpbModFun();
            modfun.module = new Buffer(this.options.precommit[i].mod);
            modfun.function = new Buffer(this.options.precommit[i].fun);
            hook.modfun = modfun;
        }
        rpbHooks[i] = hook;
    }
    return rpbHooks;
};

StoreBucketProps.prototype.onSuccess = function(rpbSetBucketResp) {
    
    // There is not response from riak except a code. rpbSetBucketResp
    // will be null upon success.
    this.options.callback(null, true);
    return true;
    
};

StoreBucketProps.prototype.onRiakError = function(rpbErrorResp) {
    this.onError(rpbErrorResp.getErrmsg().toString('utf8'));
};
    
    
StoreBucketProps.prototype.onError = function(msg) {
    this.options.callback(msg, null);
};    
    


var schema = Joi.object().keys({
    bucket: Joi.string().required(),
    bucketType: Joi.string().default('default'),
    r: Joi.number().optional(),
    pr: Joi.number().optional(),
    w: Joi.number().optional(),
    dw: Joi.number().optional(),
    pw: Joi.number().optional(),
    rw: Joi.number().optional(),
    callback: Joi.func().required(),
    notFoundOk: Joi.boolean().optional(),
    nVal: Joi.number().optional(),
    allowMult: Joi.boolean().optional(),
    lastWriteWins : Joi.boolean().optional(),
    precommit : Joi.array().optional(),
    postcommit : Joi.array().optional(),
    oldVClock : Joi.number().optional(),
    youngVClock : Joi.number().optional(),
    bigVClock : Joi.number().optional(),
    smallVClock : Joi.number().optional(),
    backend : Joi.string().optional(),
    basicQuorum : Joi.boolean().optional(),
    search : Joi.boolean().optional(),
    searchIndex : Joi.string().optional(),
    chashKeyfun : Joi.object().optional()
    
});


/**
 * A builder for constructing StoreBucketProps instances
 * * Rather than having to manually construct the __options__ and instantiating
 * a StoreBucketProps directly, this builder may be used.
 *
 * 
 * @namespace StoreBucketProps
 * @class Builder
 * @constructor 
 */
function Builder() {
    
    this.precommit = [];
    this.postcommit = [];
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
    * Set the nVal.
    * @method withNVal
    * @param {Number} nVal the number of replicas.
    * @chainable
    */
    withNVal : function(nVal) {
        this.nVal = nVal;
        return this;
    },
    /**
    * Set the allow_multi value.
    *
    * @method withAllowMult
    * @param {Boolean} allowMult whether to allow sibling objects to be created.
    * @chainable
    */
    withAllowMult : function(allowMult) {
        this.allowMult = allowMult;
        return this;
    },
    /**
    * Set the last_write_wins value. Unless you really know what you're
    * doing, you probably do not want to set this to true.
    * @method withLastWriteWins
    * @param {Boolean} lastWriteWins whether to ignore vector clocks when writing.
    * @chainable
    */
    withLastWriteWins : function(lastWriteWins) {
        this.lastWriteWins = lastWriteWins;
        return this;
    },
    /**
    * Add a pre-commit hook. 
    * 
    * @method addPrecommitHook
    * @param {Object} precommitHook the hook to add.
    * @chainable
    */
    addPrecommitHook : function(precommitHook) {
        this.precommit.push(precommitHook);
        return this;
    },
    /**
    * Add a pre-commit hook. 
    * 
    * @method addPostcommitHook
    * @param {Object} postcommitHook the hook to add.
    * @chainable
    */
    addPostcommitHook : function(postcommitHook) {
        this.postcommit.push(postcommitHook);
        return this;
    },
    /**
     * Set the old VClock value
     * @method withOldVClock
     * @param {Number} oldVClock an epoch time value
     * @chainable
     */
    withOldVClock : function(oldVClock) {
        this.oldVClock = oldVClock;
        return this;
    },
    /**
     * Set the young VClock value
     * @param {Number} youngVClock an epoch time value
     * @chainable
     */
    withYoungVClock : function(youngVClock) {
        this.youngVClock = youngVClock;
        return this;
    },
    /**
     * Set the big VClock value.
     * @param {Number} bigVClock an epoch time value
     * @chainable
     */
    withBigVClock : function(bigVClock) {
        this.bigVClock = bigVClock;
        return this;
    },
    /**
     * Set the small VClock value.
     * @param {Number} smallVClock an epoch time value.
     * @chainable
     */
    withSmallVClock : function(smallVClock) {
        this.smallVClock = smallVClock;
        return this;
    },
    /**
    * Set the backend used by this bucket. 
    * Only applies when using
    * riak_kv_multi_backend in Riak.
    * @method withBackend
    * @param {Sring} backend the name of the backend to use.
    * @chainable
    */ 
    withBackend : function(backend) {
        this.backend = backend;
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
    * Set the basic_quorum value.
    * The parameter controls whether a read request should return early in
    * some fail cases. 
    * E.g. If a quorum of nodes has already
    * returned notfound/error, don't wait around for the rest.
    * @method withBasicQuorum
    * @param {Boolean} useBasicQuorum the basic_quorum value.
    * @chainable
    */
    withBasicQuorum : function(useBasicQuorum) {
        this.basicQuorum = useBasicQuorum;
        return this;
    },
    /**
    * Set the not_found_ok value.
    * If true a vnode returning notfound for a key increments the r tally.
    * False is higher consistency, true is higher availability.
    * @method withNotFoundOk
    * @param {Boolean} notFoundOk the not_found_ok value.
    * @chainable
    */
    withNotFoundOk : function(notFoundOk) {
        this.notFoundOk = notFoundOk;
        return this;
    },
    /**
    * Enable Legacy Riak Search. Setting this to true causes the search
    * pre-commit hook to be added.
    *
    * Note this is only for legacy Riak (&lt; v2.0) Search support.
    * @method withLegacySearchEnabled
    * @param {Boolean} search enable add/remove (true/false) the pre-commit hook for Legacy
    * Riak Search.
    * @chainable
    */
    withSearch : function(search) {
        this.search = search;
        return this;
    },
    /**
    * Associate a Search Index. 
    * This only applies if Yokozuna is enabled in
    * Riak v2.0.
    * @method withSearchIndex
    * @param {String} indexName The name of the search index to use.
    * @chainable
    */
    withSearchIndex : function(indexName) {
        this.searchIndex = indexName;
        return this;
    },
    /**
     * Set the chash_keyfun value.
     * 
     * @method withChashkeyFunction
     * @param {Object} func a object representing the Erlang func to use.
     * @chainable
     */
    withChashkeyFunction : function(func) {
        this.chashKeyfun = func;
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
     * Construct a StoreBucketProps command.
     * @return {StoreBucketProps}
     */
    build : function() {
        return new StoreBucketProps(this);
    }
    
};

module.exports = StoreBucketProps;
module.exports.Builder = Builder;