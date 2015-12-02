'use strict';

var CommandBase = require('../commands/commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');

/**
 * @module Core
 */

/**
 * Provides the StartTls command used to start a TLS session with Riak.
 * @class StartTls
 * @constructor
 * @param {Function} callback the function to call when the command completes or errors
 * @extends CommandBase
 */
function StartTls(callback) {
    CommandBase.call(this, 'RpbStartTls', 'RpbStartTls', callback);

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

module.exports = StartTls;

