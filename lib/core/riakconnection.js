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
var logger = require('winston');

var DEFAULT_MAX_BUFFER = 2048 * 1024;
var DEFAULT_INIT_BUFFER = 2 * 1024;
var DEFAULT_CONNECTION_TIMEOUT = 30000;

function RiakConnection(options) {

    // https://nodejs.org/api/tls.html#tls_tls_connect_options_callback
    this.tls_options = {
        host: 'riak-test',
        port: 10017,
        // key: fs.readFileSync(''),
        // cert: fs.readFileSync('')
        pfx: fs.readFileSync('C:/Users/lbakken/Projects/basho/riak-dotnet-client/tools/test-ca/certs/riakuser-client-cert.pfx'),
        ca: [ fs.readFileSync('C:/Users/lbakken/Projects/basho/riak-dotnet-client/tools/test-ca/certs/cacert.pem') ],
        rejectUnauthorized: false //,
        // secureProtocol: 'TLSv1_method'
    };
  
    events.EventEmitter.call(this);
    
    this.remoteAddress = options.remoteAddress;
    this.remotePort = options.remotePort;
    this.connectionTimeout = options.connectionTimeout;
    
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
}

util.inherits(RiakConnection, events.EventEmitter);

RiakConnection.prototype.connect = function() {
    // Send RpbStartTls, expect RpbSstartTls message code back
    this._connection.on('error', this._connectionError.bind(this));
    this._connection.on('timeout', this._connectionTimeout.bind(this));
    this._connection.setTimeout(this.connectionTimeout);
    this._connection.connect(this.remotePort, this.remoteAddress, this._connected.bind(this));
};

RiakConnection.prototype._connected = function() {
    logger.info("Connected; host: " + this.remoteAddress + 
                " port: " + this.remotePort);
    this._connection.removeListener('error', this._connectionError);
    this._connection.on('error', this._socketError.bind(this));
    this._connection.on('close', this._connClosed.bind(this));
    this._connection.on('data', this._receiveData.bind(this));
    this._connection.setTimeout(0);
    this.emit('connected', this);
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
    
RiakConnection.prototype._receiveData = function(data) {

    // Create a new buffer to receive data if needed 
    if (this._buffer === null) {
        this._buffer = new ByteBuffer(this.initBufferSize);
    }

    this._buffer.append(data);
    this._buffer.flip();
    var protobufArray = this._getProtobufsFromBuffer();
    
    for (var i = 0; i < protobufArray.length; i++) {
        this.emit('responseReceived', this, this.command, 
                    protobufArray[i].msgCode, protobufArray[i].protobuf);
    }
    
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
            var code = slice.readInt8();
            
            // Our fun API does some creative things like ... returning only 
            // a code, with 0 bytes following. In those cases we want to set 
            // decoded to null.
            var decoded = null;
            if (messageLength - 1 > 0) {
                var ResponseProto = RiakProtobuf.getProtoFor(code);
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
    this.inFlight = true;
    this.lastUsed = Date.now();
    // write PB to socket
    var message = command.getRiakMessage();
    this._connection.write(message.header);
    this._connection.write(message.protobuf);
    return true;
};

module.exports = RiakConnection;

