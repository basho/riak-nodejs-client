'use strict';

var Joi = require('joi');
var assert = require('assert');

describe('Joi', function() {
    it('returns object being validated on success', function(done) {
        var cb = function (err, rslt) { };
        var schema = Joi.func().required();
        var self = this;
        Joi.validate(cb, schema, function(err, value) {
            if (err) {
                throw 'callback is required and must be a function.';
            }
            assert.strictEqual(value, cb);
            done();
        });
    });
});
