var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');
var ByteBuffer = require('bytebuffer');

var Rpb = require('../../protobuf/riakprotobuf');
var DtOp = Rpb.getProtoFor('DtOp');
var SetOp = Rpb.getProtoFor('SetOp');

function UpdateSet(options, callback) {
    CommandBase.call(this, 'DtUpdateReq', 'DtUpdateResp', callback);

    var self = this;
    Joi.validate(options, schema, function(err, options) {
        if (err) {
            throw err;
        }
        self.options = options;
    });
}

inherits(UpdateSet, CommandBase);

function pbuf(self, prop) {
    return new Buffer(self.options[prop]);
}

function bufferize(thing) {
    if (typeof thing === "string") return new Buffer(thing);
    if (thing instanceof Buffer) return thing;
    if (thing instanceof ByteBuffer) return thing.toBuffer();

    var mesg = "Couldn't bufferize " + (typeof thing) + " " + thing;
    throw mesg;
}

function buflist(list) {
    var newList = [];
    var len = list.length;
    for (var i = 0; i < len; i++) {
        newList[i] = bufferize(list[i]);
    }
    return newList;
}

function stringlist(bufferList) {
    var newList = [];
    var len = bufferList.length;
    for (var i = 0; i < len; i++) {
        newList[i] = bufferList[i].toString();
    }
    return newList;
}

UpdateSet.prototype.constructPbRequest = function() {
    var protobuf = this.getPbReqBuilder();

    // namespace
    protobuf.setType(pbuf(this, 'bucketType'));
    protobuf.setBucket(pbuf(this, 'bucket'));
    protobuf.setKey(pbuf(this, 'key'));

    // operation
    var op = new DtOp();
    protobuf.setOp(op);
    var setOp = new SetOp();
    op.setSetOp(setOp);
    setOp.adds = buflist(this.options.additions);
    setOp.removes = buflist(this.options.removals);

    // quorum
    protobuf.setW(this.options.w);
    protobuf.setDw(this.options.dw);
    protobuf.setPw(this.options.pw);

    // options
    protobuf.setReturnBody(this.options.returnBody);
    protobuf.setTimeout(this.options.timeout);
    protobuf.setIncludeContext(this.options.includeContext);

    return protobuf;
};

UpdateSet.prototype.onSuccess = function(dtUpdateResp) {
    var response = {};

    if (dtUpdateResp.getKey()) {
        response.key = dtUpdateResp.getKey();
        response.keyString = response.key.toString("utf8");
    }

    if (dtUpdateResp.getContext()) {
        response.context = dtUpdateResp.getContext();
    }

    if (dtUpdateResp.getSetValue()) {
        // set a dataType field so we look like a fetchset response
        response.dataType = 2;
        response.valueBuffers = buflist(dtUpdateResp.getSetValue());
        response.value = stringlist(response.valueBuffers);
    }

    this._callback(null, response);
    return true;
};

var schema = Joi.object().keys({
    callback: Joi.func().strip().optional(),

    //namespace
    bucket: Joi.string().required(),
    // bucket type is required since default probably shouldn't have a
    // datatype associated with it
    bucketType: Joi.string().required(),
    key: Joi.string().required(),

    //operations
    additions: Joi.array().default([]).optional(),
    removals: Joi.array().default([]).optional(),

    context: Joi.default(null).optional(),

    //quorum
    w: Joi.number().default(null).optional(),
    dw: Joi.number().default(null).optional(),
    pw: Joi.number().default(null).optional(),

    //options
    returnBody: Joi.boolean().default(true).optional(),
    timeout: Joi.number().default(null).optional(),
    includeContext: Joi.boolean().default(true).optional()
});

/**
 * A builder for constructing UpdateSet instances.
 * * Rather than having to manually construct the __options__ and instantiating
 * a UpdateSet directly, this builder may be used.
 *
 *      var UpdateSet = require('./lib/commands/datatype/updateset');
 *      var update = new UpdateSet.Builder()
 *                       .withBucketType('myBucketType')
 *                       .withBucket('myBucket')
 *                       .withKey('myKey')
 *                       .withCallback(myCallback)
 *                       .build();
 *
 * @namespace UpdateSet
 * @class Builder
 * @constructor
 */
function Builder() {}

Builder.prototype = {
    /**
     * Construct a SetchSet instance.
     * @method build
     * @return {FetchSet}
     */
    build: function() {
        return new UpdateSet(this, this.callback);
    }
};

function firstUc(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}

function bb(prop) {
    Builder.prototype["with"+firstUc(prop)] = function(pv) {
        this[prop] = pv;
        return this;
    };
}

bb('callback');
/**
* Set the bucket type.
* @method withBucketType
* @param {String} bucketType the bucket type in riak
* @chainable
*/
bb('bucketType');
/**
* Set the bucket.
* @method withBucket
* @param {String} bucket the bucket in Riak
* @chainable
*/
bb('bucket');
/**
* Set the key.
* @method withKey
* @param {String} key the key in riak.
* @chainable
*/
bb('key');
/**
* Set the causal context. The context is necessary for set removals. It is
* an opaque field, and should not be parsed or modified.
* @method withContext
* @param {ByteBuffer} context the causal context
* @chainable
 */
bb('context');
/**
* Set the W value.
* If not set the bucket default is used.
* @method withW
* @param {Number} w the W value.
* @chainable
*/
bb('w');
/**
* Set the DW value.
* If not set the bucket default is used.
* @method withDw
* @param {Number} dw the DW value.
* @chainable
*/
bb('dw');
/**
* Set the PW value.
* If not set the bucket default is used.
* @method withPw
* @param {Number} pw the PW value.
* @chainable
*/
bb('pw');
/**
* Set the return_body value.
* If true, the callback is passed the contents of the set after the update.
* @method withReturnBody
* @param {Boolean} returnBody the return_body value.
* @chainable
*/
bb('returnBody');
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
bb('useBasicQuorum');
/**
* Set a timeout for this operation.
* @method withTimeout
* @param {Number} timeout a timeout in milliseconds.
* @chainable
*/
bb('timeout');

bb('additions');
bb('removals');

module.exports = UpdateSet;
module.exports.Builder = Builder;
