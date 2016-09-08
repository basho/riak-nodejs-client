'use strict';

var rpb = require('../../protobuf/riakprotobuf');
var inherits = require('util').inherits;
var Joi = require('joi');

var CommandBase = require('../commandbase');

var RpbBucketProps = rpb.getProtoFor('RpbBucketProps');
var RpbCommitHook = rpb.getProtoFor('RpbCommitHook');
var RpbModFun = rpb.getProtoFor('RpbModFun');

/**
 * Base class for StoreBucketProps and StoreBucketTypeProps classes.
 * @module KV
 */

/**
 * @class StorePropsBase
 * @constructor
 * @param {Object} options The properties to store
 * @param {Number} [options.r] The R value.
 * @param {Number} [options.pr] The PR value.
 * @param {Number} [options.w] The W value.
 * @param {Number} [options.dw] The DW value.
 * @param {Number} [options.pw] The PW value.
 * @param {Number} [options.rw] The RW value.
 * @param {Boolean} [options.notFoundOk] If true a vnode returning notfound for a key increments the r tally.
 * @param {Boolean} [options.basicQuorum] Controls whether a read request should return early in some fail cases.
 * @param {Number} [options.nVal] The number of replicas.
 * @param {Boolean} [options.allowMult] Whether to allow sibling objects to be created.
 * @param {Boolean} [options.lastWriteWins] Whether to ignore vector clocks when writing.
 * @param {Number} [options.oldVClock] An epoch time value.
 * @param {Number} [options.youngVClock] An epoch time value.
 * @param {Number} [options.bigVClock] An epoch time value.
 * @param {Number} [options.smallVClock] An epoch time value.
 * @param {Sring} [options.backend] The name of the backend to use.
 * @param {Boolean} [options.search] Enable the pre-commit hook for Legacy Riak search.
 * @param {String} [options.searchIndex] The name of the search index to use.
 * @param {Object} [options.chashKeyfun] An object representing the Erlang func to use.
 * @param {Object[]} [options.precommit] Array of precommit hooks
 * @param {Object[]} [options.postcommit] Array of postcommit hooks
 * @param {Number} [options.hllPrecision] The number of bits to use in the HyperLogLog precision.
 * @param {String} pbRequestName name of the Riak protocol buffer this command will send
 * @param {String} pbResponseName name of the Riak protocol buffer this command will receive
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Boolean} callback.response the response from Riak. This will be true.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will ne null if no error.
 * @param {Object} callback.response The response from Riak. This is an oject with all the bucket properties.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function StorePropsBase(options, pbRequestName, pbResponseName, callback) {
    CommandBase.call(this, pbRequestName, pbResponseName, callback);
    this.validateOptions(options, schema);
}

inherits(StorePropsBase, CommandBase);

StorePropsBase.prototype.constructPbRequest = function() {

    var protobuf = this.getPbReqBuilder();

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

    props.setHllPrecision(this.options.hllPrecision >= 0 ? this.options.hllPrecision : null);

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

StorePropsBase.prototype._convertHooks = function(hooks) {
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

StorePropsBase.prototype.onSuccess = function(rpbSetBucketResp) {
    // There is not response from riak except a code. rpbSetBucketResp
    // will be null upon success.
    this._callback(null, true);
    return true;
};

var schema = Joi.object().keys({
    // dervied classes validate these
    bucket: Joi.string().optional(),
    bucketType: Joi.string().optional(),
    // StorePropsBase validates the rest
    r: Joi.number().optional(),
    pr: Joi.number().optional(),
    w: Joi.number().optional(),
    dw: Joi.number().optional(),
    pw: Joi.number().optional(),
    rw: Joi.number().optional(),
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
    chashKeyfun : Joi.object().optional(),
    hllPrecision : Joi.number().min(4).max(16).optional()
});

function Builder() {}

/**
 * Set the bucket type.
 * If not supplied, 'default' is used.
 * @method withBucketType
 * @param {String} bucketType the bucket type in riak
 * @chainable
 */
Builder.prototype.withBucketType = function(bucketType) {
    this.bucketType = bucketType;
    return this;
};

/**
 * Set the nVal.
 * @method withNVal
 * @param {Number} nVal the number of replicas.
 * @chainable
 */
Builder.prototype.withNVal = function(nVal) {
    this.nVal = nVal;
    return this;
};

/**
 * Set the allow_multi value.
 *
 * @method withAllowMult
 * @param {Boolean} allowMult whether to allow sibling objects to be created.
 * @chainable
 */
Builder.prototype.withAllowMult = function(allowMult) {
    this.allowMult = allowMult;
    return this;
};

/**
 * Set the last_write_wins value. Unless you really know what you're
 * doing, you probably do not want to set this to true.
 * @method withLastWriteWins
 * @param {Boolean} lastWriteWins whether to ignore vector clocks when writing.
 * @chainable
 */
Builder.prototype.withLastWriteWins = function(lastWriteWins) {
    this.lastWriteWins = lastWriteWins;
    return this;
};

/**
 * Add a pre-commit hook.
 *
 * @method addPrecommitHook
 * @param {Object} precommitHook the hook to add.
 * @chainable
 */
Builder.prototype.addPrecommitHook = function(precommitHook) {
    this.precommit.push(precommitHook);
    return this;
};

/**
 * Add a pre-commit hook.
 *
 * @method addPostcommitHook
 * @param {Object} postcommitHook the hook to add.
 * @chainable
 */
Builder.prototype.addPostcommitHook = function(postcommitHook) {
    this.postcommit.push(postcommitHook);
    return this;
};

/**
 * Set the old VClock value
 * @method withOldVClock
 * @param {Number} oldVClock an epoch time value
 * @chainable
 */
Builder.prototype.withOldVClock = function(oldVClock) {
    this.oldVClock = oldVClock;
    return this;
};

/**
 * Set the young VClock value
 * @method withYoungVClock
 * @param {Number} youngVClock an epoch time value
 * @chainable
 */
Builder.prototype.withYoungVClock = function(youngVClock) {
    this.youngVClock = youngVClock;
    return this;
};

/**
 * Set the big VClock value.
 * @method withBigVClock
 * @param {Number} bigVClock an epoch time value
 * @chainable
 */
Builder.prototype.withBigVClock = function(bigVClock) {
    this.bigVClock = bigVClock;
    return this;
};

/**
 * Set the small VClock value.
 * @method withSmallVClock
 * @param {Number} smallVClock an epoch time value.
 * @chainable
 */
Builder.prototype.withSmallVClock = function(smallVClock) {
    this.smallVClock = smallVClock;
    return this;
};

/**
 * Set the backend used by this bucket.
 * Only applies when using
 * riak_kv_multi_backend in Riak.
 * @method withBackend
 * @param {Sring} backend the name of the backend to use.
 * @chainable
 */
Builder.prototype.withBackend = function(backend) {
    this.backend = backend;
    return this;
};

/**
 * Set the R value.
 * If not asSet the bucket default is used.
 * @method withR
 * @param {Number} r the R value.
 * @chainable
 */
Builder.prototype.withR = function(r) {
    this.r = r;
    return this;
};

/**
* Set the PR value.
* If not asSet the bucket default is used.
* @method withPr
* @param {Number} pr the PR value.
* @chainable
*/
Builder.prototype.withPr = function(pr) {
    this.pr = pr;
    return this;
};

/**
 * Set the W value.
 * How many replicas to write to before returning a successful response.
 * If not set the bucket default is used.
 * @method withW
 * @param {number} w the W value.
 * @chainable
 */
Builder.prototype.withW = function(w) {
    this.w = w ;
    return this;
};

/**
 * Set the DW value.
 * How many replicas to commit to durable storage before returning a successful response.
 * If not set the bucket default is used.
 * @method withDw
 * @param {number} dw the DW value.
 * @chainable
 */
Builder.prototype.withDw = function(dw) {
    this.dw = dw;
    return this;
};

/**
 * Set the PW value.
 * How many primary nodes must be up when the write is attempted.
 * If not set the bucket default is used.
 * @method withPw
 * @param {number} pw the PW value.
 * @chainable
 */
Builder.prototype.withPw = function(pw) {
    this.pw = pw;
    return this;
};

/**
 * Set the RW value.
 * Quorum for both operations (get and put) involved in deleting an object .
 * @method withRw
 * @param {number} rw the RW value.
 * @chainable
 */
Builder.prototype.withRw = function(rw) {
    this.rw = rw;
    return this;
};

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
Builder.prototype.withBasicQuorum = function(useBasicQuorum) {
    this.basicQuorum = useBasicQuorum;
    return this;
};

/**
 * Set the not_found_ok value.
 * If true a vnode returning notfound for a key increments the r tally.
 * False is higher consistency, true is higher availability.
 * @method withNotFoundOk
 * @param {Boolean} notFoundOk the not_found_ok value.
 * @chainable
 */
Builder.prototype.withNotFoundOk = function(notFoundOk) {
    this.notFoundOk = notFoundOk;
    return this;
};

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
Builder.prototype.withSearch = function(search) {
    this.search = search;
    return this;
};

/**
 * Associate a Search Index.
 * This only applies if Yokozuna is enabled in
 * Riak v2.0.
 * @method withSearchIndex
 * @param {String} searchIndex The name of the search index to use.
 * @chainable
 */
Builder.prototype.withSearchIndex = function(searchIndex) {
    this.searchIndex = searchIndex;
    return this;
};

/**
 * Set the chash_keyfun value.
 *
 * @method withChashkeyFunction
 * @param {Object} func a object representing the Erlang func to use.
 * @chainable
 */
Builder.prototype.withChashkeyFunction = function(func) {
    this.chashKeyfun = func;
    return this;
};

/**
 * Set the Hyperloglog Precision value
 * @method withHllPrecision
 * @param {Number} precision the number of bits to use in the HyperLogLog precision.
 *     Valid values are [4 - 16] inclusive, default is 14 on new buckets.
 *     <b>NOTE:</b> When changing precision, it may only be reduced from
 *     it's current value, and never increased.
 * @chainable
 */
Builder.prototype.withHllPrecision = function(precision) {
    this.hllPrecision = precision;
    return this;
};

/**
 * Set the callback to be executed when the operation completes.
 * @method withCallback
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will ne null if no error.
 * @param {Object} callback.response The response from Riak. This is an oject with all the bucket properties.
 * @chainable
 */
Builder.prototype.withCallback = function(callback) {
    this.callback = callback;
    return this;
};

module.exports = StorePropsBase;
module.exports.Builder = Builder;
