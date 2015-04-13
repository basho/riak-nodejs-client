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

    var self = this;
    Joi.validate(options, schema, function(err, options) {
        if (err) {
            throw err;
        }
        self.user = options.user;
        self.password = options.password;
    });
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

