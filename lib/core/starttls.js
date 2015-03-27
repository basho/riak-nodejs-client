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

/**
 * Provides the StartTls command used to start a TLS session with Riak.
 * 
 * @module StartTls
 * @class StartTls
 * @constructor
 * @param {Object} options
 * @param {Function} options.callback the function to call when the command completes or errors
 * @extends CommandBase
 */ 
function StartTls(options) {
    CommandBase.call(this, 'RpbStartTls', 'RpbStartTls');

    if (options) {
        var self = this;
        Joi.validate(options, schema, function(err, options) {
            if (err) {
                throw err;
            }
            if (options.callback) {
                self.callback = options.callback;
            }
        });
    }
}

inherits(StartTls, CommandBase);

StartTls.prototype.constructPbRequest = function() {
    /*
     * NB: since this is just a message code there is nothing to return
     */
    return;
};

StartTls.prototype.onSuccess = function(rpbStartTlsResp) {
        
    if (rpbStartTlsResp === null) {
        // TODO ERROR
    }

    return true;
};
    
StartTls.prototype.onRiakError = function(rpbErrorResp) {
    this.onError(rpbErrorResp.getErrmsg().toString('utf8'));
};
    
    
StartTls.prototype.onError = function(msg) {
    if (this.callback) {
        this.callback(msg, null);
    }
};

var schema = Joi.object().keys({
    callback: Joi.func().optional()
});

module.exports = StartTls;

