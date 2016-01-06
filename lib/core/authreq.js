'use strict';

var CommandBase = require('../commands/commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');
var logger = require('winston');

/**
 *
 * @module Core
 */

/**
 * Provides the AuthReq class
 * Command used to authenticate with Riak.
 *
 * @class AuthReq
 * @constructor
 * @param {Object} options
 * @param {String} options.user the user with which to authenticate (required)
 * @param {String} options.password the password with which to authenticate (optional)
 * @param {Function} callback the function to call when the command completes or errors
 * @extends CommandBase
 */
function AuthReq(options, callback) {
    CommandBase.call(this, 'RpbAuthReq', 'RpbAuthResp', callback);
    this.validateOptions(options, schema);
    this.user = this.options.user;
    this.password = this.options.password;
}

inherits(AuthReq, CommandBase);

AuthReq.prototype.constructPbRequest = function() {
    var protobuf = this.getPbReqBuilder();

    protobuf.setUser(new Buffer(this.user));
    protobuf.setPassword(new Buffer(this.password));

    return protobuf;
};

AuthReq.prototype.onSuccess = function(rpbAuthResp) {
    /*
     * NB: this method is never called since AuthReq
     * is "internally" used only by RiakConnection
     */
    return true;
};

var schema = Joi.object().keys({
    user: Joi.string().required(),
    password: Joi.string().optional().allow('').default('')
});

module.exports = AuthReq;
