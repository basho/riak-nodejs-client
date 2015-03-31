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
var Joi = require('joi');
var inherits = require('util').inherits;

/**
 * Provides the Ping class
 * @module Ping
 */

/**
 * Command used to ping Riak.
 * 
 * @class Ping
 * @constructor
 * @param {Function} callback the callback to be executed when the operation completes.
 * @extends CommandBase
 */ 
function Ping(callback) {
    CommandBase.call(this, 'RpbPingReq', 'RpbPingResp', callback);
}

inherits(Ping, CommandBase);

Ping.prototype.constructPbRequest = function() {
    /*
     * NB: since this is just a message code there is nothing to return
     */
    return;
};

Ping.prototype.onSuccess = function(rpbPingResp) {
    this._callback(null, true);
    return true;
};
    
module.exports = Ping;

