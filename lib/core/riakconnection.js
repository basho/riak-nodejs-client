'use strict';

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
var DEFAULT_CONNECTION_TIMEOUT = 3000;

// NB: fixes GH 104
// https://github.com/basho/riak-nodejs-client/issues/104
// TODO FUTURE: remove this when Riak uses Erlang R17 or higher.
var RIAK_R16_CIPHERS = 'DHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA256:DHE-RSA-AES256-SHA:AES128-SHA256:AES128-SHA:AES256-SHA256:AES256-SHA:RC4-SHA';

/**
 * @module Core
 */

/**
 * Provides the RiakConnection class.
 * @class RiakConnection
 * @constructor
 * @param {Object} options - the options to use.
 */
function RiakConnection(options) {
    events.EventEmitter.call(this);

    this.remoteAddress = options.remoteAddress;
    this.remotePort = options.remotePort;
    this.connectionTimeout = options.connectionTimeout;

    if (options.cork) {
        this.cork = true;
    }

    if (options.auth) {
        this.auth = options.auth;
        this.auth.ciphers = RIAK_R16_CIPHERS;
    }

    if (options.healthCheck) {
        this.healthCheck = options.healthCheck;
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

    this.closed = false;
    this._connection = new net.Socket();
    if (this._connection.setKeepAlive) {
        this._connection.setKeepAlive(true, 0);
    }
    if (this._connection.setNoDelay) {
        this._connection.setNoDelay(true);
    }

    // Note: useful for debugging event-related nonsense
    // this._connection.setMaxListeners(1);

    if (this.cork && !this._connection.cork) {
        logger.warn('[RiakConnection] wanted to use cork/uncork but not supported!');
        this.cork = false;
    } else {
        logger.debug('[RiakConnection] using cork() / uncork()');
    }

    this._emitAndClose = function(evt, evt_args) {
        if (!this.closed) {
            this.closed = true;
            this._connection.end();
            this.emit(evt, this, evt_args);
            this.close();
        }
    };

    this._connHandleEnd = function () {
        logger.debug('[RiakConnection] handling "end" event');
        this._emitAndClose('connectionClosed');
    };
}

util.inherits(RiakConnection, events.EventEmitter);

RiakConnection.prototype.connect = function() {
    this._boundConnectionError = this._connectionError.bind(this);
    this._connection.on('error', this._boundConnectionError);
    this._connection.on('timeout', this._connectionTimeout.bind(this));

    // TODO FUTURE: this *is* the read/write timeout as well as idle timeout
    // https://nodejs.org/api/net.html#net_socket_settimeout_timeout_callback
    this._connection.setTimeout(this.connectionTimeout);

    this._connection.connect(this.remotePort, this.remoteAddress, this._connected.bind(this));
};

RiakConnection.prototype._connected = function() {
    logger.debug('[RiakConnection] (%s:%d) connected',
        this.remoteAddress, this.remotePort);

    this._connection.removeListener('error', this._boundConnectionError);
    this._connection.on('error', this._socketError.bind(this));
    this._connection.on('end', this._connHandleEnd.bind(this));

    if (this.auth) {
        /*
         * NB: at this point, we have not yet emitted the 'connected' event,
         * so listeners will not have yet registered for 'connectionClosed'.
         * This is why the 'close' event must raise 'connectFailed' via
         * _boundConnectionError
         */
        logger.debug('[RiakConnection] StartTls');
        this._connection.on('close', this._boundConnectionError);
        this._boundReceiveStartTls = this._receiveStartTls.bind(this);
        this._connection.on('data', this._boundReceiveStartTls);
        var command = new StartTls(function(){});
        this.execute(command);
    } else if (this.healthCheck) {
         // NB: see above comment re: 'close' event
        logger.debug('[RiakConnection] HealthCheck');
        this._connection.on('close', this._boundConnectionError);
        this._boundReceiveHealthCheck = this._receiveHealthCheck.bind(this);
        this._connection.on('data', this._boundReceiveHealthCheck);
        this.execute(this.healthCheck);
    } else {
        this._connection.on('close', this._connClosed.bind(this));
        this._connection.on('data', this._receiveData.bind(this));

        // TODO FUTURE: this *is* the read/write timeout as well as idle timeout
        // https://nodejs.org/api/net.html#net_socket_settimeout_timeout_callback
        this._connection.setTimeout(0);

        logger.debug('[RiakConnection] emit connected, no-auth');
        this.emit('connected', this);
    }
};

RiakConnection.prototype._connectionError = function(err) {
    this._emitAndClose('connectFailed', err);
};

RiakConnection.prototype._connectionTimeout = function(err) {
    if (!err) {
        err = 'timed out or other error trying to connect';
    }
    this._connectionError(err);
};

RiakConnection.prototype._socketError = function(err) {
    // This is only called if we have an error after a successful connection
    // log only because close will be called right after
    // https://nodejs.org/api/net.html#net_event_error
    if (err) {
        logger.error('[RiakConnection] (%s:%d) _socketError:', this.remoteAddress, this.remotePort, err);
    }
};

RiakConnection.prototype._receiveHealthCheck = function(data) {
    logger.debug('[RiakConnection]: receive healthcheck response');
    var msgName = this.healthCheck.name;
    var expectedCode = this.healthCheck.expectedCode;
    if (this._ensureExpectedResponse(data, msgName, expectedCode)) {
        this._connection.removeListener('close', this._boundConnectionError);
        this._connection.on('close', this._connClosed.bind(this));

        // On data, use the _receiveData function
        this._connection.removeListener('data', this._boundReceiveHealthCheck);
        this._connection.on('data', this._receiveData.bind(this));

        // TODO FUTURE: this *is* the read/write timeout as well as idle timeout
        // https://nodejs.org/api/net.html#net_socket_settimeout_timeout_callback
        this._connection.setTimeout(0);

        this.inFlight = false;
        logger.debug('[RiakConnection] emit connected, healthcheck success');
        this.emit('connected', this);
    }
};

RiakConnection.prototype._receiveStartTls = function(data) {
    logger.debug('[RiakConnection]: receive StartTls response');
    var expectedCode = RiakProtoBuf.getCodeFor('RpbStartTls');
    if (this._ensureExpectedResponse(data, 'RpbStartTls', expectedCode)) {
        var tls_secure_context = tls.createSecureContext(this.auth);
        var tls_socket_options = {
            isServer: false, // NB: required
            secureContext: tls_secure_context
        };

        this._connection = new tls.TLSSocket(this._connection, tls_socket_options);

        var auth_options = {
            user: this.auth.user,
            password: this.auth.password
        };

        // On data, move to next sequence in TLS negotiation
        this._connection.removeListener('data', this._boundReceiveStartTls);
        this._boundReceiveAuthResp = this._receiveAuthResp.bind(this);
        this._connection.on('data', this._boundReceiveAuthResp);

        // Execute AuthReq command
        this.inFlight = false;
        var command = new AuthReq(auth_options);
        this.execute(command);
    }
};

RiakConnection.prototype._receiveAuthResp = function(data) {
    logger.debug('[RiakConnection]: receive RpbAuthResp');
    var expectedCode = RiakProtoBuf.getCodeFor('RpbAuthResp');
    if (this._ensureExpectedResponse(data, 'RpbAuthResp', expectedCode)) {
        this._connection.removeListener('close', this._boundConnectionError);
        this._connection.on('close', this._connClosed.bind(this));

        this._connection.removeListener('data', this._boundReceiveAuthResp);
        this._connection.on('data', this._receiveData.bind(this));

        // TODO FUTURE: this *is* the read/write timeout as well as idle timeout
        // https://nodejs.org/api/net.html#net_socket_settimeout_timeout_callback
        this._connection.setTimeout(0);

        this.inFlight = false;

        logger.debug('[RiakConnection] emit connected, with-auth');
        this.emit('connected', this);
    }
};

RiakConnection.prototype._receiveData = function(data) {
    var protobufArray = this._buildProtobufArray(data);

    for (var i = 0; i < protobufArray.length; i++) {
        this.emit('responseReceived', this, this.command, 
                    protobufArray[i].msgCode, protobufArray[i].protobuf);
    }
};

RiakConnection.prototype._ensureExpectedResponse = function(data, msgName, expectedCode) {
    var protobufArray = this._buildProtobufArray(data);
    var err;
    if (protobufArray.length === 0) {
        err = 'Expected ' + msgName + ' response message';
    } else {
        var resp = protobufArray[0];
        if (resp.msgCode === 0) {
            // We received an RpbErrorResp
            err = resp.protobuf.getErrmsg().toString('utf8');
        } else if (resp.msgCode !== expectedCode) {
            err = msgName + ' incorrect response code: ' + resp.msgCode;
        }
    }

    if (err) {
        // TODO: not really a connection error, but may prevent bugs
        // from affecting data?
        this._connectionError(err);
        return false;
    } else {
        return true;
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
        var messageLength = this._buffer.readUint32();

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
                // GH issue #45
                // Must use 'true' as argument to force copy of data
                // otherwise, subsequent fetches will clobber data
                decoded = ResponseProto.decode(slice.toBuffer(true));
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

// TODO FUTURE: what does "had_error" really mean?
RiakConnection.prototype._connClosed = function(had_error) {
    this._emitAndClose('connectionClosed');
};

RiakConnection.prototype.close = function() {
    this.closed = true;
    this.removeAllListeners();
    this._buffer = null;
    if (this._connection) {
        this._connection.end();
        this._connection.removeAllListeners();
        this._connection.on('error', function (err) {
            if (err) {
                logger.error('[RiakConnection] error AFTER close:', err);
            }
        });
        this._connection = null;
    }
};

// command includes user callback
RiakConnection.prototype.execute = function(command) {
    this.command = command;

    if (this.inFlight === true) {
        logger.error('[RiakConnection] attempted to run command on in-use connection');
        return false;
    }

    logger.debug('[RiakConnection] execute command:', command.name);
    this.inFlight = true;
    this.lastUsed = Date.now();
    // write PB to socket
    var message = command.getRiakMessage();

    /*
     * Use of cork()/uncork() suggested by Doug Luce
     * https://github.com/dougluce
     * https://github.com/basho/riak-nodejs-client/pull/56
     * https://github.com/basho/riak-nodejs-client/pull/57
     */
    if (this.cork) {
        this._connection.cork();
    }

    this._connection.write(message.header);

    if (message.protobuf) {
        this._connection.write(message.protobuf);
    }

    if (this.cork) {
        this._connection.uncork();
    }

    return true;
};

module.exports = RiakConnection;
