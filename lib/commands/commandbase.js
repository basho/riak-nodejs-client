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

CommandBase.prototype.getRiakMessage = function() {
    var encoded = this.protobuf.encode().toBuffer();
    this.header.writeInt32BE(encoded.length + 1, 0);
    return {
        protobuf: encoded,
        header: this.header
    };
};

CommandBase.prototype.getExpectedResponseCode = function() {
        return this.expectedCode;
};

CommandBase.prototype.getRiakMessage = function() {
    var encoded = this.constructPbRequest().encode().toBuffer();
    this.header.writeInt32BE(encoded.length + 1, 0);
    return {
        protobuf: encoded,
        header: this.header
    };
};

CommandBase.prototype.getPbReqBuilder = function() {
    return new this.pbBuilder();
};

CommandBase.prototype.constructPbRequest = function() {
    throw 'Not supported yet!';
};

CommandBase.prototype.onSuccess = function(pbResponseMessage) {
    throw 'Not supported yet!';
};

CommandBase.prototype.onRiakError = function(rpbErrorResp) {
    throw 'Not supported yet!';
};

CommandBase.prototype.onError = function(msg) {
    throw 'Not supported yet!';
};

var schema = Joi.object().keys({
    callback: Joi.func().required()
});

module.exports = CommandBase;

