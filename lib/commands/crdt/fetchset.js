var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');

function FetchSet(options) {
    CommandBase.call(this, 'DtFetchReq', 'DtFetchResp');

    var self = this;
    Joi.validate(options, schema, function(err, options) {
        if (err) {
            throw err;
        }
        self.options = options;
    });
}

inherits(FetchSet, CommandBase);

function buf(self, prop) {
    return new Buffer(self.options[prop]);
}

FetchSet.prototype.constructPbRequest = function() {
    var protobuf = this.getPbReqBuilder();

    // namespace
    protobuf.setType(buf(this, 'bucketType'));
    protobuf.setBucket(buf(this, 'bucket'));
    protobuf.setKey(buf(this, 'key'));

    // quorum
    protobuf.setR(this.options.r);
    protobuf.setPr(this.options.pr);
    protobuf.setNotfoundOk(this.options.notFoundOk);
    protobuf.setBasicQuorum(this.options.useBasicQuorum);

    protobuf.setTimeout(this.options.timeout);

    // experimental
    protobuf.setSloppyQuorum(this.options.sloppyQuorum);
    protobuf.setNVal(this.options.nValue);

    return protobuf;
};

FetchSet.prototype.onSuccess = function(dtFetchResp) {
    var response = {
        // treat context as opaque, don't string-ify
        context: dtFetchResp.getContext(),
        dataType: dtFetchResp.getType(),
        value: dtFetchResp.getValue().getSetValue()
    };

    this.options.callback(null, response);
};

FetchSet.prototype.onRiakError = function(rpbErrorResp) {
    this.onError(rpbErrorResp.getErrmsg().toString('utf8'));
};

FetchSet.prototype.onError = function(msg) {
    this.options.callback(msg, null);
};

var schema = Joi.object().keys({
    callback: Joi.func().required(),

    // namespace
    bucket: Joi.string().required(),
    // bucket type is required since default probably shouldn't have a
    // datatype associated with it
    bucketType: Joi.string().required(),
    key: Joi.string().required(),

    // quorum
    r: Joi.number().default(null).optional(),
    pr: Joi.number().default(null).optional(),
    notFoundOk: Joi.boolean().default(null).optional(),
    useBasicQuorum: Joi.boolean().default(null).optional(),

    timeout: Joi.number().default(null).optional(),

    // experimental
    sloppyQuorum: Joi.boolean().default(null).optional(),
    nValue: Joi.number().default(null).optional()
});

function Builder() {}

Builder.prototype = {
    build: function() {
        return new FetchSet(this);
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
bb('bucketType');
bb('bucket');
bb('key');
bb('r');
bb('pr');
bb('notFoundOk');
bb('useBasicQuorum');
bb('timeout');
bb('sloppyQuorum');
bb('nValue');

module.exports = FetchSet;
module.exports.Builder = Builder;
