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
    console.log(this.options);
    protobuf.setType(buf(this, 'bucketType'));
    protobuf.setBucket(buf(this, 'bucket'));
    protobuf.setKey(buf(this, 'key'));
    
    return protobuf;
};

var schema = Joi.object().keys({
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

bb('bucketType');
bb('bucket');
bb('key');

module.exports = FetchSet;
module.exports.Builder = Builder;
