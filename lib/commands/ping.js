'use strict';

var CommandBase = require('../commands/commandbase');
var inherits = require('util').inherits;

/**
 * Provides the Ping class
 * @module Core
 */

/**
 * Command used to ping Riak.
 * @class Ping
 * @constructor
 * @param {Function} callback the callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Boolean} callback.response the response from Riak. Will be true unless there was an error.
 * @param {Object} callback.data additional error data. Will be null if no error.
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

