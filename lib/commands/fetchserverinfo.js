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

