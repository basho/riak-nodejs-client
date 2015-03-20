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
 * Provides the AuthReq class
 * @module AuthReq
 */

/**
 * Command used to authenticate a with Riak.
 * 
 * @class AuthReq
 * @constructor
 * @param {Object} options
 * @param {String} options.user the user with which to authenticate.
 * @param {String} options.password the password with which to authenticate.
 * @extends CommandBase
 */ 
function AuthReq(options) {

    CommandBase.call(this, 'RpbAuthReq', 'RpbAuthResp');

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
    
    logger.debug("setting password to: '" + this.password + "'");
    protobuf.setPassword(new Buffer(this.password));

    return;
};

AuthReq.prototype.onSuccess = function(rpbAuthResp) {
        
    if (rpbAuthResp === null) {
        // TODO
        logger.error("AuthReq no rpbAuthResp");
    } else {
        var pbContentArray = rpbGetResp.getContent();
        if (pbContentArray.length === 0) {
            logger.debug("pbContentArray is empty");
        } else {
            logger.debug("pbContentArray is NOT empty");
        }
    }

    return true;
};
    
AuthReq.prototype.onRiakError = function(rpbErrorResp) {
    this.onError(rpbErrorResp.getErrmsg().toString('utf8'));
};
    
    
AuthReq.prototype.onError = function(msg) {
    this.callback(msg, null);
};

var schema = Joi.object().keys({
   user: Joi.string().required(),
   password: Joi.string().optional().allow('').default('')
});

module.exports = AuthReq;

