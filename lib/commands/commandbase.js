'use strict';

var util = require('util');
var ProtoBufFactory = require('../protobuf/riakprotobuf');
var Joi = require('joi');

var cid = 1;

/**
 * Provides a base class for all commands.
 * 
 * @module Core
 */

/**
 * Base class for all commands. 
 * 
 * Classes extending this need to override:
 * 
 * constructPbRequest
 * onSuccess
 * onRiakError
 * onError
 * 
 * @class CommandBase
 * @constructor
 * @param {String} pbRequestName name of the Riak protocol buffer this command will send
 * @param {String} pbResponseName name of the Riak protocol buffer this command will receive
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response the response from Riak.
 * @param {Object} callback.data additional error data. Will be null if no error.
 */
function CommandBase(pbRequestName, pbResponseName, callback) {
    var requestCode = ProtoBufFactory.getCodeFor(pbRequestName);
    this.expectedCode = ProtoBufFactory.getCodeFor(pbResponseName);
    this.pbBuilder = ProtoBufFactory.getProtoFor(pbRequestName);

    var schema = Joi.func().required();
    var self = this;
    Joi.validate(callback, schema, function(err, option) {
       if (err) {
           throw new Error('callback is required and must be a function.');
       }
       self.callback = callback;
    });
   
    this.header = new Buffer(5);
    this.header.writeUInt8(requestCode, 4);
    
    this.remainingTries = 1;
    
    // This is to facilitate debugging what pb messages this Command represents
    this.name = util.format('%s-%d', pbRequestName, cid);
    cid++;

    this.validateOptions = function (options, schema, joi_opts) {
        var self = this;
        Joi.validate(options, schema, joi_opts, function(err, options) {
            if (err) {
                throw err;
            }
            self.options = options;
        });
    };
}

/**
 * Fires the user's callback with the arguments passed in.
 * @method getCallback
 * @private
 * @return {Function} the user supplied callback
 */
CommandBase.prototype._callback = function() {
    this.callback.apply(this, arguments);
};

/**
 * Returns the expected response code for this command.
 * @method getExpectedResponseCode
 * @private
* @return {number} the expected response code for this command.
 */
CommandBase.prototype.getExpectedResponseCode = function() {
        return this.expectedCode;
};

/**
 * Returns the encoded protobuf and message header.
 * @private
 * @method getRiakMessage
 * @return {Object} object containing the header and encoded protobuf
 */
CommandBase.prototype.getRiakMessage = function() {
    var encoded;
    var pbRequest = this.constructPbRequest();

    if (pbRequest) {
        encoded = pbRequest.encode().toBuffer();
        this.header.writeInt32BE(encoded.length + 1, 0);
    } else {
        this.header.writeInt32BE(1, 0);
    }

    return {
        protobuf: encoded,
        header: this.header
    };
};

/**
 * Returns and instance of the protocol buffer message builder for this command.
 * This is determined via the pbRequestName passed to the constructor.
 * @private
 * @method getPbReqBuilder
 * @return {Object} the builder for the protocol buffer message to be sent for this command
 */
CommandBase.prototype.getPbReqBuilder = function() {
    var pbReqBuilder;

    if (this.pbBuilder) {
        pbReqBuilder = new this.pbBuilder();
    }

    return pbReqBuilder;
};

/** 
 * Construct and return the Riak protocol buffer message for this command.
 * Subclasses must override this method.
 * @protected
 * @method constructPbRequest
 * @return {Object} a protocol buffer message builder
 */
CommandBase.prototype.constructPbRequest = function() {
    throw new Error('Not supported yet!');
};

/**
 * Called by RiakNode when a response is received.
 * Subclasses must override this method.
 * @protected
 * @method onSuccess
 * @param {Object} pbResponseMessage the protocol buffer received from riak
 * @return {Boolean} true if not streaming or the last response has been received, false otherwise.
 */
CommandBase.prototype.onSuccess = function(pbResponseMessage) {
    throw new Error('Not supported yet!');
};

/**
 * Called by RiakNode when a RpbErrorResp is received and all retries are exhausted.
 * Commands may override this method.
 * @protected
 * @method onRiakError
 * @param {Object} rpbErrorResp the RpbErrorResp protocol buffer
 */
CommandBase.prototype.onRiakError = function(rpbErrorResp) {
    var errmsg = rpbErrorResp.getErrmsg().toString('utf8');
    var errcode = rpbErrorResp.getErrcode();
    this.onError(errmsg, { msg: errmsg, code: errcode });
};

/**
 * Called by RiakNode if an error occurs executing the command and all retries are exhausted.
 * @protected
 * @method onError
 * @param {String} msg an error message
 * @param {Object} data additional error data
 */
CommandBase.prototype.onError = function(msg, data) {
    this._callback(msg, null, data);
};

var schema = Joi.object().keys({
    callback: Joi.func().required()
});

module.exports = CommandBase;
