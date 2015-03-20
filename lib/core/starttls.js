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

/**
 * Provides the StartTls class
 * @module StartTls
 *
 */

/**
 * Command used to start a TLS session with Riak.
 * 
 * @class StartTls
 * @constructor
 * @extends CommandBase
 * 
 */ 
function StartTls() {
    CommandBase.call(this, 'RpbStartTls', 'RpbStartTls');
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
    this.callback(msg, null);
};

module.exports = StartTls;

