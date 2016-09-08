'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;

/**
 * Base class for FetchBucketProps and FetchBucketTypeProps classes.
 * @module KV
 */

/**
 * @class FetchPropsBase
 * @constructor
 * @param {String} pbRequestName name of the Riak protocol buffer this command will send
 * @param {String} pbResponseName name of the Riak protocol buffer this command will receive
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will ne null if no error.
 * @param {Object} callback.response The response from Riak. This is an oject with all the bucket properties.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function FetchPropsBase(pbRequestName, pbResponseName, callback) {
    CommandBase.call(this, pbRequestName, pbResponseName, callback);
}

inherits(FetchPropsBase, CommandBase);

FetchPropsBase.prototype.onSuccess = function(rpbGetBucketResp) {

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
        repl: rpbBucketProps.getRepl(),
        hllPrecision: rpbBucketProps.getHllPrecision()
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

FetchPropsBase.prototype._parseHooks = function(rpbCommitHooks) {
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

module.exports = FetchPropsBase;
module.exports.Builder = Builder;
