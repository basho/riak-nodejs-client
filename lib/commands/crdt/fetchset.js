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
};

FetchSet.prototype.constructPbRequest = function() {
    var protobuf = this.getPbReqBuilder();
    protobuf.setType(buf(this, 'bucketType'));
    protobuf.setBucket(buf(this, 'bucket'));
    protobuf.setKey(buf(this, 'key'));
    
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

var schema = Joi.object().keys({
    callback: Joi.func().required(),
    
    // namespace
    bucket: Joi.string().required(),
    // bucket type is required since default probably shouldn't have a
    // datatype associated with it
    bucketType: Joi.string().required(),
    key: Joi.string().required(),

    // quorum
    r: Joi.number().optional(),
    pr: Joi.number().optional(),
    notFoundOk: Joi.boolean().optional(),
    useBasicQuorum: Joi.boolean().optional(),

    timeout: Joi.number().optional(),

    // experimental
    sloppyQuorum: Joi.boolean().optional(),
    nValue: Joi.number().optional()
});

function Builder() {};

Builder.prototype = {
    build: function() {
        return new FetchSet(this);
    }
};

function firstUc(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
};

function bb(prop) {
    Builder.prototype["with"+firstUc(prop)] = function(pv) {
        this[prop] = pv;
        return this;
    };
};

bb('callback');
bb('bucketType');
bb('bucket');
bb('key');

module.exports = FetchSet;
module.exports.Builder = Builder;
