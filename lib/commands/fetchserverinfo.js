/**
 *
 * Copyright 2014-present Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

var CommandBase = require('../commands/commandbase');
var inherits = require('util').inherits;

/**
 * Provides the FetchServerInfo class
 * @module Core
 */

/**
 * Command used to ping Riak.
 * @class FetchServerInfo
 * @constructor
 * @param {Function} callback the callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Boolean} callback.response the response from Riak. Will be true unless there was an error.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */ 
function FetchServerInfo(callback) {
    CommandBase.call(this, 'RpbGetServerInfoReq', 'RpbGetServerInfoResp', callback);
}

inherits(FetchServerInfo, CommandBase);

FetchServerInfo.prototype.constructPbRequest = function() {
    /*
     * NB: since this is just a message code there is nothing to return
     */
    return;
};

FetchServerInfo.prototype.onSuccess = function(rpbFetchServerInfoResp) {
    var response = { node: '', server_version: '' };
    if (rpbFetchServerInfoResp) {
        var node = rpbFetchServerInfoResp.getNode().toString('utf8');
        var server_version = rpbFetchServerInfoResp.getServerVersion().toString('utf8');
        response.node = node;
        response.server_version = server_version;
    }
    this._callback(null, response);
    return true;
};
    
module.exports = FetchServerInfo;

