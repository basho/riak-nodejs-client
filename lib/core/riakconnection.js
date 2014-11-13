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
var events = require('events');
var util = require('util');
var ByteBuffer = require('bytebuffer');
var RiakProtobuf = require('../protobuf/riakprotobuf');

var DEFAULT_MAX_BUFFER = 2048 * 1024;
var DEFAULT_INIT_BUFFER = 2 * 1024;
var DEFAULT_CONNECTION_TIMEOUT = 3000;

function RiakConnection(options) {
  
    events.EventEmitter.call(this);
    
    this.remoteAddress = options.remoteAddress;
    this.remotePort = options.remotePort;
    this.connectionTimeout = options.connectionTimeout;
    
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
    
}

util.inherits(RiakConnection, events.EventEmitter);


RiakConnection.prototype.connect = function() {
    this._connection.on('error', this._connectionError.bind(this));
    this._connection.connect(this.remotePort, this.remoteAddress, this._connected.bind(this));
};

RiakConnection.prototype._connected = function() {
    console.log("Connected; host: " + this.remoteAddress + 
                " port: " + this.remotePort);
    this._connection.removeListener('error', this._connectionError);
    this._connection.on('error', this._socketError.bind(this));
    this._connection.on('close', this._connClosed.bind(this));
    this._connection.on('data', this._receiveData.bind(this));
    this.emit('connected', this);
};

RiakConnection.prototype._connectionError = function(err) {
    // Connection error, emit to listener
    console.log("Failed to connect;" + 
                this.remoteAddress + 
                " port: " + this.remotePort + 
                " error: " + err);
    this._connection.destroy();
    this.emit('connectFailed', this, err);
};
    
RiakConnection.prototype._socketError = function(err) {
    // This is only called if we have an error after a successful connection
    // log only because close will be called right after
    console.log("Socket error; " + err);
};
    
RiakConnection.prototype._receiveData = function(data) {

    // Create a new buffer to receive data if needed 
    if (this._buffer === null) {
        this._buffer = new ByteBuffer(this.initBufferSize);
    }

    this._buffer.append(data);
    this._buffer.mark();
    this._buffer.flip();

    // Need 4 byte min for the length
    if (this._buffer.remaining() >= 4) {

        var messageLength = this._buffer.readInt32();

        // See if we have the complete message
        if (this._buffer.remaining() < messageLength) {
            // Nope. Flip it back and reset the offset.
            this._buffer.flip();
            this._buffer.reset();
        } else {
            // We have a complete message from riak
            var slice = this._buffer.slice(undefined, messageLength + 4);
            var code = slice.readInt8();
            var ResponseProto = RiakProtobuf.getProtoFor(code);
            var decoded = ResponseProto.decode(slice.toBuffer());

            // Copy any remainder to start of buffer, set offset
            this._buffer.skip(messageLength);
            if (this._buffer.remaining()) {
                var newOffset = this._buffer.remaining();
                this._buffer.copyTo(this._buffer, 0);
                this._buffer.offset = newOffset;
                this._buffer.limit = this._buffer.capacity();
            } else {
                this._buffer.flip();
            }
            
            this.emit('responseReceived', this, this.command, code, decoded);
            //this._handleRiakResponse(code, decoded);
            
        }
    } else {
        // Don't have 4 bytes yet; flip and reset to mark
        this._buffer.flip();
        this._buffer.reset();
    }
};
    
//RiakConnection.prototype._handleRiakResponse = function(code, decoded) {
//
//    if (code === RiakProtobuf.getCodeFor('RpbErrorResp'))
//    {
//        this.command.onError(error);
//        this._resetBuffer();
//        this.inFlight = false;
//        this.emit('operationComplete', this, false);
//        
//    } else if (code !== this.command.getExpectedResponseCode()) {
//        // Uh oh.
//        var msg = "Received incorrect response code; expected: " +
//                this.command.getExpectedResponseCode + 
//                " received: " +
//                code;
//        // TODO: Erm, what do we do here? Just log? Callback? Dump the connection?
//        this.command.callback(msg, null);
//        this._resetBuffer();
//        this.inFlight = false;
//        this.emit('operationComplete', this, false);
//    } else {
//        // Got the right response.
//        var ResponseProto = RiakProtobuf.getProtoFor(code);
//        var done = this.command.onSuccess(ResponseProto.decode(slice.toBuffer()));
//
//        // Check if we're done here
//        if (done) {
//            // we're done.
//            this.inFlight = false;
//            this._resetBuffer();
//            this.emit('operationComplete', this, true);
//        } 
//    }
//};
    
RiakConnection.prototype.resetBuffer = function() {
    if (this._buffer.capacity() > this.maxBufferSize) {
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
    // TODO: Check to see if in use (never should be)
    if (this.inFlight === true) {
        console.log("Attempted to run command on in-use connection");
        return false;
    }
    this.inFlight = true;
    this.lastUsed = Date.now();
    // write PB to socket
    var message = command.getRiakMessage();
    this._connection.write(message.header);
    this._connection.write(message.protobuf);
    return true;
};

module.exports = RiakConnection;

