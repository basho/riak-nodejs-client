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

var ProtoBufFactory = require('../protobuf/riakprotobuf');
var Joi = require('joi');

/**
 * Provides a base class for all commands.
 * 
 * @module CommandBase
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
 */
function CommandBase(pbRequestName, pbResponseName) {
    
    var requestCode = ProtoBufFactory.getCodeFor(pbRequestName);
    this.expectedCode = ProtoBufFactory.getCodeFor(pbResponseName);
    this.pbBuilder = ProtoBufFactory.getProtoFor(pbRequestName);
   
    this.header = new Buffer(5);
    this.header.writeUInt8(requestCode, 4);
    
    this.remainingTries = 1;
    
}

/**
 * Returns the expected response code for this command.
 * @method getExpectedResponseCode
 * @returns {number} the expected response code for this command.
 */
CommandBase.prototype.getExpectedResponseCode = function() {
        return this.expectedCode;
};

/**
 * Returns the encoded protobuf and message header.
 * @method getRiakMessage
 * @returns {Object} object containing the header and encoded protobuf
 */
CommandBase.prototype.getRiakMessage = function() {
    var encoded = this.constructPbRequest().encode().toBuffer();
    this.header.writeInt32BE(encoded.length + 1, 0);
    return {
        protobuf: encoded,
        header: this.header
    };
};

/**
 * Returns and instance of the protocol buffer message builder for this command.
 * This is determined via the pbRequestName passed to the constructor.
 * @method getPbReqBuilder
 * @returns {Object} the builder for the protocol buffer message to be sent for this command
 */
CommandBase.prototype.getPbReqBuilder = function() {
    return new this.pbBuilder();
};

/** 
 * Construct and return the Riak protocol buffer message for this command.
 * Commands must override this method.
 * @method constructPbRequest
 * @returns {Object} a protocol buffer message builder
 */
CommandBase.prototype.constructPbRequest = function() {
    throw 'Not supported yet!';
};

/**
 * Called by RiakNode when a response is received.
 * Commands must override this method.
 * @method onSuccess
 * @param {Object} pbResponseMessage the protocol buffer received from riak
 * @returns {Boolean} true if not streaming or the last response has been received, false otherwise.
 */
CommandBase.prototype.onSuccess = function(pbResponseMessage) {
    throw 'Not supported yet!';
};

/**
 * Called by RiakNode when a RpbErrorResp is received and all retries are exhausted.
 * Commands must override this method.
 * @method onRiakError
 * @param {Object} rpbErrorResp the RpbErrorResp protocol buffer
 */
CommandBase.prototype.onRiakError = function(rpbErrorResp) {
    throw 'Not supported yet!';
};

/**
 * Called by Riaknode if an error occurs executing the command and all retries are exhausted.
 * @method onError
 * @param {String} msg an error message
 */
CommandBase.prototype.onError = function(msg) {
    throw 'Not supported yet!';
};

var schema = Joi.object().keys({
    callback: Joi.func().required()
});

module.exports = CommandBase;

