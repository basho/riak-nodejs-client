/*
 * Copyright 2014 Basho Technologies, Inc.
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

var net = require('net');
var tls = require('tls');
var events = require('events');
var util = require('util');
var fs = require('fs');
var logger = require('winston');

var ByteBuffer = require('bytebuffer');
var RiakProtoBuf = require('../protobuf/riakprotobuf');
var StartTls = require('./starttls');
var AuthReq = require('./authreq');

var DEFAULT_MAX_BUFFER = 2048 * 1024;
var DEFAULT_INIT_BUFFER = 2 * 1024;
var DEFAULT_CONNECTION_TIMEOUT = 30000;

function RiakConnection(options) {
  
    events.EventEmitter.call(this);
    
    this.remoteAddress = options.remoteAddress;
    this.remotePort = options.remotePort;
    this.connectionTimeout = options.connectionTimeout;

    if (options.auth) {
        this.auth = options.auth;
    }
    
    if (options.hasOwnProperty('connectionTimeout')) {
        this.connectionTimeout = options.connectionTimeout;
    } else {
        this.connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
    }
    
    if (options.hasOwnProperty('maxBufferSize')) {
        this.maxBufferSize = options.maxBufferSize;
    } else {
        this.maxBufferSize = DEFAULT_MAX_BUFFER;
    }
    
    if (options.hasOwnProperty('initBufferSize')) {
        this.initBufferSize = options.initBufferSize;
    } else {
        this.initBufferSize = DEFAULT_INIT_BUFFER;
    }
    
    this.inFlight = false; 
    this.lastUsed = Date.now();
    
    this._buffer = null;
    
    this._connection = new net.Socket();
    // Note: useful for debugging event-related nonsense
    // this._connection.setMaxListeners(1);
}

util.inherits(RiakConnection, events.EventEmitter);

RiakConnection.prototype.connect = function() {
    this._boundConnectionError = this._connectionError.bind(this);
    this._connection.on('error', this._boundConnectionError);
    this._connection.on('timeout', this._connectionTimeout.bind(this));
    this._connection.setTimeout(this.connectionTimeout);
    this._connection.connect(this.remotePort, this.remoteAddress, this._connected.bind(this));
};

RiakConnection.prototype._connected = function() {
    logger.info("Connected; host: " + this.remoteAddress + 
                " port: " + this.remotePort);

    this._connection.removeListener('error', this._boundConnectionError);
    this._connection.on('error', this._socketError.bind(this));
    this._connection.on('close', this._connClosed.bind(this));

    if (this.auth) {
        // On data, move to next sequence in TLS negotiation
        logger.debug('RiakConnection:_connected:StartTls');
        this._boundReceiveStartTls = this._receiveStartTls.bind(this);
        this._connection.on('data', this._boundReceiveStartTls);
        var command = new StartTls(function(){});
        this.execute(command);
    } else {
        this._connection.on('data', this._receiveData.bind(this));
        this._connection.setTimeout(0);
        logger.debug('RiakConnection:emit:connected:no-auth');
        this.emit('connected', this);
    }
};

RiakConnection.prototype._connectionError = function(err) {
    // Connection error, emit to listener
    logger.error("Failed to connect;" + 
                this.remoteAddress + 
                " port: " + this.remotePort + 
                " error: " + err);
    this._connection.destroy();
    this.emit('connectFailed', this, err);
};
    
RiakConnection.prototype._connectionTimeout = function() {
    this._connectionError('Timed out trying to connect');
};
    
RiakConnection.prototype._socketError = function(err) {
    // This is only called if we have an error after a successful connection
    // log only because close will be called right after
    logger.error("Socket error; " + err);
};
    
RiakConnection.prototype._receiveStartTls = function(data) {

    logger.debug("RiakConnection:_receiveStartTls");

    this._ensureExpectedResponse(data, 'RpbStartTls');

    var tls_secure_context = tls.createSecureContext(this.auth);
    var tls_socket_options = {
        isServer: false, // NB: required
        secureContext: tls_secure_context,
    };

    this._connection = new tls.TLSSocket(this._connection, tls_socket_options);

    var auth_options = {
        user: this.auth.user,
        password: this.auth.password,
    };

    // On data, move to next sequence in TLS negotiation
    this._connection.removeListener('data', this._boundReceiveStartTls);
    this._boundReceiveAuthResp = this._receiveAuthResp.bind(this);
    this._connection.on('data', this._boundReceiveAuthResp);

    // Execute AuthReq command
    this.inFlight = false;
    var command = new AuthReq(auth_options, function(){});
    this.execute(command);
};

RiakConnection.prototype._receiveAuthResp = function(data) {

    this._ensureExpectedResponse(data, 'RpbAuthResp');

    // On data, use the _receiveData function
    this._connection.removeListener('data', this._boundReceiveAuthResp);
    this._connection.on('data', this._receiveData.bind(this));
    this._connection.setTimeout(0);

    this.inFlight = false;

    logger.debug('RiakConnection:emit:connected:yes-auth');
    this.emit('connected', this);

};

RiakConnection.prototype._receiveData = function(data) {

    var protobufArray = this._buildProtobufArray(data);
    
    for (var i = 0; i < protobufArray.length; i++) {
        this.emit('responseReceived', this, this.command, 
                    protobufArray[i].msgCode, protobufArray[i].protobuf);
    }

};

RiakConnection.prototype._ensureExpectedResponse = function(data, msgName) {

    var protobufArray = this._buildProtobufArray(data);
    if (protobufArray.length === 0) {
        throw new Error('Expected ' + msgName + ' response message');
    }

    var resp = protobufArray[0];
    var code = RiakProtoBuf.getCodeFor(msgName);
    if (resp.msgCode !== code) {
        var err = msgName + ' incorrect response code: ' + resp.msgCode;
        logger.error(err);
        throw new Error(err);
    }

};
    
RiakConnection.prototype._buildProtobufArray = function(data) {

    // Create a new buffer to receive data if needed 
    if (this._buffer === null) {
        this._buffer = new ByteBuffer(this.initBufferSize);
    }

    this._buffer.append(data);
    this._buffer.flip();
    return this._getProtobufsFromBuffer();

};

RiakConnection.prototype._getProtobufsFromBuffer = function(protobufArray) {
    
    if (arguments.length === 0) {
        protobufArray = [];
    }
    
    if (this._buffer.remaining() >= 4) {
        this._buffer.mark();
        var messageLength = this._buffer.readInt32();
        
        // See if we have the complete message
        if (this._buffer.remaining() >= messageLength) {
            // We have a complete message from riak
            var slice = this._buffer.slice(undefined, this._buffer.offset + messageLength);
            var code = slice.readUint8();
            
            // Our fun API does some creative things like ... returning only 
            // a code, with 0 bytes following. In those cases we want to set 
            // decoded to null.
            var decoded = null;
            if (messageLength - 1 > 0) {
                var ResponseProto = RiakProtoBuf.getProtoFor(code);
                decoded = ResponseProto.decode(slice.toBuffer());
            } 
            
            protobufArray[protobufArray.length] = { msgCode : code, protobuf : decoded};
            // skip past message in buffer
            this._buffer.skip(messageLength);
            // recursively call this until we are out of messages
            return this._getProtobufsFromBuffer(protobufArray);
        } else {
            // rewind the offset 
            this._buffer.reset();
        }   
    }
    
    // ByteBuffer's 'flip()' effectively clears the buffer which we don't
    // want. We want to flip while preserving anything in the buffer and 
    // compact if necessary.
    
    var newOffset = this._buffer.remaining();
    // Compact if necessary
    if (newOffset > 0 && this._buffer.offset !== 0) {
        this._buffer.copyTo(this._buffer, 0);
    }
    this._buffer.offset = newOffset;
    this._buffer.limit = this._buffer.capacity();
    
    return protobufArray;
};
    
RiakConnection.prototype.resetBuffer = function() {
    if (this._buffer && this._buffer.capacity() > this.maxBufferSize) {
        this._buffer = null;
    } 
};
    
RiakConnection.prototype._connClosed = function(had_error) {
    this._connection.destroy();
    this.emit('connectionClosed', this);
};
    
RiakConnection.prototype.close = function() {
    this.removeAllListeners();
    this._connection.removeAllListeners();
    this._connection.destroy();
    this._buffer = null;
};
    
// command includes user callback
RiakConnection.prototype.execute = function(command) {
    this.command = command;
    if (this.inFlight === true) {
        logger.error("Attempted to run command on in-use connection");
        return false;
    }
    logger.debug("RiakConnection:execute:command: " + command.PbRequestName);
    this.inFlight = true;
    this.lastUsed = Date.now();
    // write PB to socket
    var message = command.getRiakMessage();
    this._connection.write(message.header);
    if (message.protobuf) {
        this._connection.write(message.protobuf);
    }
    return true;
};

module.exports = RiakConnection;

