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
