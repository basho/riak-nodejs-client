'use strict';

var net = require('net');
var tls = require('tls');
var events = require('events');
var util = require('util');
var fs = require('fs');
var logger = require('winston');

var ByteBuffer = require('bytebuffer');
var StartTls = require('./starttls');
var AuthReq = require('./authreq');
var utils = require('./utils');

var rpb = require('../protobuf/riakprotobuf');

var DEFAULT_MAX_BUFFER = 2048 * 1024;
var DEFAULT_INIT_BUFFER = 2 * 1024;

// TODO FUTURE these are shared with RiakNode
var DEFAULT_CONNECTION_TIMEOUT = 3000;
var DEFAULT_REQUEST_TIMEOUT = 5000;

// NB: fixes GH 104
// https://github.com/basho/riak-nodejs-client/issues/104
// TODO FUTURE: remove this when Riak uses Erlang R17 or higher.
var RIAK_R16_CIPHERS = 'DHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA256:DHE-RSA-AES256-SHA:AES128-SHA256:AES128-SHA:AES256-SHA256:AES256-SHA:RC4-SHA';

var cid = {};

function debugOutputConnectionListeners(name, conn) {
    if (logger.debug) {
        logger.debug('%s listeners for "close" event: %d', name, conn.listenerCount('close'));
        logger.debug('%s listeners for "connect" event: %d', name, conn.listenerCount('connect'));
        logger.debug('%s listeners for "data" event: %d', name, conn.listenerCount('data'));
        logger.debug('%s listeners for "drain" event: %d', name, conn.listenerCount('drain'));
        logger.debug('%s listeners for "end" event: %d', name, conn.listenerCount('end'));
        logger.debug('%s listeners for "error" event: %d', name, conn.listenerCount('error'));
        logger.debug('%s listeners for "lookup" event: %d', name, conn.listenerCount('lookup'));
        logger.debug('%s listeners for "timeout" event: %d', name, conn.listenerCount('timeout'));
    }
}

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

    // This is to facilitate debugging
    if (!cid[this.remotePort]) {
        cid[this.remotePort] = 1;
    }
    this.name = util.format('[RiakConnection] (%s:%d-%d)',
        this.remoteAddress, this.remotePort, cid[this.remotePort]);
    cid[this.remotePort]++;

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

    this.connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
    if (options.hasOwnProperty('connectionTimeout')) {
        this.connectionTimeout = options.connectionTimeout;
    }

    this.requestTimeout = DEFAULT_REQUEST_TIMEOUT;
    if (options.hasOwnProperty('requestTimeout')) {
        this.requestTimeout = options.requestTimeout;
    }

    this.maxBufferSize = DEFAULT_MAX_BUFFER;
    if (options.hasOwnProperty('maxBufferSize')) {
        this.maxBufferSize = options.maxBufferSize;
    }

    var initBufferSize = DEFAULT_INIT_BUFFER;
    if (options.hasOwnProperty('initBufferSize')) {
        initBufferSize = options.initBufferSize;
    }

    this._emitAndClose = function(evt, evt_args) {
        if (!this.closed) {
            // NB: this can be useful
            // logger.debug("%s emitting '%s' args '%s'", this.name, evt, evt_args);
            // NB: RiakNode checks inFlight to re-try command if necessary
            // so don't set inFlight to false here, it will be set to false in close()
            this.closed = true;
            this._connection.end();
            this.emit(evt, this, evt_args);
            this.close();
        }
    };

    this._connHandleEnd = function () {
        logger.debug('%s handling "end" event', this.name);
        this._emitAndClose('connectionClosed');
    };

    this._connHandleTimeout = function (command) {
        var err = util.format("%s command '%s' timed out (in-flight: %s)",
            this.name, command.name, this.inFlight);
        if (logger.debug) {
            logger.debug(err);
        }
        this._emitAndClose('connectionClosed');
    };

    this._clearSocketTimeout = function() {
        if (this._connection) {
            if (this._boundConnectionTimeout) {
                this._connection.removeListener('timeout', this._boundConnectionTimeout);
                this._boundConnectionTimeout = null;
            }
            this._connection.setTimeout(0);
        }
    };

    // buffer is private
    var buffer = null;

    // private buffer functions
    function initBuffer(data) {
        // Create a new buffer to receive data if needed
        if (buffer === null) {
            buffer = new ByteBuffer(initBufferSize);
        }
        buffer.append(data);
        buffer.flip();
    }

    function getProtobufsFromBuffer(protobufArray) {
        if (arguments.length === 0) {
            protobufArray = [];
        }

        if (buffer.remaining() >= 4) {
            buffer.mark();
            var messageLength = buffer.readUint32();

            // See if we have the complete message
            if (buffer.remaining() >= messageLength) {
                // We have a complete message from riak
                var slice = buffer.slice(undefined, buffer.offset + messageLength);
                var code = slice.readUint8();

                // Our fun API does some creative things like ... returning only
                // a code, with 0 bytes following. In those cases we want to set
                // decoded to null.
                var decoded = null;
                if (messageLength - 1 > 0) {
                    var ResponseProto = rpb.getProtoFor(code);
                    // GH issue #45
                    // Must use 'true' as argument to force copy of data
                    // otherwise, subsequent fetches will clobber data
                    decoded = ResponseProto.decode(slice.toBuffer(true));
                }

                protobufArray[protobufArray.length] = { msgCode : code, protobuf : decoded };
                // skip past message in buffer
                buffer.skip(messageLength);
                // recursively call this until we are out of messages
                return getProtobufsFromBuffer(protobufArray);
            } else {
                // rewind the offset
                buffer.reset();
            }
        }

        // ByteBuffer's 'flip()' effectively clears the buffer which we don't
        // want. We want to flip while preserving anything in the buffer and
        // compact if necessary.

        var newOffset = buffer.remaining();
        // Compact if necessary
        if (newOffset > 0 && buffer.offset !== 0) {
            buffer.copyTo(buffer, 0);
        }
        buffer.offset = newOffset;
        buffer.limit = buffer.capacity();

        return protobufArray;
    }

    function closeBuffer() {
        if (buffer) {
            buffer.clear();
            buffer = null;
        }
    }

    // protected buffer functions
    this._closeBuffer = function () {
        closeBuffer();
    };

    this._resetBuffer = function () {
        if (buffer && buffer.capacity() > this.maxBufferSize) {
            closeBuffer();
        }
    };

    this._buildProtobufArray = function (data) {
        initBuffer(data);
        return getProtobufsFromBuffer();
    };

    // protected execute functions
    this._executeInit = function() {
        this.lastUsed = Date.now();
        this.executeDone();
    };

    this._executeStart = function(command) {
        this.command = command;
        logger.debug('%s execute command:', this.name, command.name);
        this.inFlight = true;
        this.lastUsed = Date.now();
    };

    this._executeInit();

    this.closed = false;
    this._connectedEmitted = false;

    this._connection = new net.Socket();
    if (this._connection.setKeepAlive) {
        this._connection.setKeepAlive(true, 0);
    }
    if (this._connection.setNoDelay) {
        this._connection.setNoDelay(true);
    }

    // Note: useful for debugging event issues
    /*
    debugOutputConnectionListeners(this.name, this._connection);
    this.setMaxListeners(1);
    this._connection.setMaxListeners(1);
    */

    if (this.cork && !this._connection.cork) {
        logger.warn('%s wanted to use cork/uncork but not supported!', this.name);
        this.cork = false;
    } else {
        logger.debug('%s using cork() / uncork()', this.name);
    }
}

util.inherits(RiakConnection, events.EventEmitter);

RiakConnection.prototype.connect = function() {
    this._boundConnectionError = this._connectionError.bind(this);
    this._connection.on('error', this._boundConnectionError);

    // This *is* the read/write timeout as well as idle timeout
    // https://nodejs.org/api/net.html#net_socket_settimeout_timeout_callback
    this._boundConnectionTimeout = this._connectionTimeout.bind(this);
    this._connection.setTimeout(this.connectionTimeout, this._boundConnectionTimeout);

    this._boundConnected = this._connected.bind(this);
    this._connection.connect(this.remotePort, this.remoteAddress, this._boundConnected);
};

RiakConnection.prototype._connected = function() {
    logger.debug('%s connected', this.name);
    this._connection.removeListener('connect', this._boundConnected);
    this._boundConnected = null;

    this._connection.removeListener('error', this._boundConnectionError);
    this._boundConnectionError = this._socketError.bind(this);
    this._connection.on('error', this._boundConnectionError);

    this._connection.on('end', this._connHandleEnd.bind(this));
    this._connection.on('data', this._receiveData.bind(this));

    if (this.auth) {
        /*
         * NB: at this point, we have not yet emitted the 'connected' event,
         * so listeners will not have yet registered for 'connectionClosed'.
         * This is why the 'close' event must raise 'connectFailed' via
         * _boundConnectionError
         */
        this._connection.on('close', this._boundConnectionError);
        this._boundResponseReceived = this._receiveStartTls.bind(this);
        this.on('responseReceived', this._boundResponseReceived);
        var command = new StartTls();
        this.execute(command);
    } else if (this.healthCheck) {
         // NB: see above comment re: 'close' event
        this._connection.on('close', this._boundConnectionError);
        this._boundResponseReceived = this._receiveHealthCheck.bind(this);
        this.on('responseReceived', this._boundResponseReceived);
        this.execute(this.healthCheck);
    } else {
        this._clearSocketTimeout();
        this._connection.on('close', this._connClosed.bind(this));
        logger.debug('%s emit connected, no-auth', this.name);
        this._connectedEmitted = true;
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
        logger.error('%s _socketError:', this.name);
    }
};

RiakConnection.prototype._receiveHealthCheck = function(conn, command, code, decoded) {
    // NB: this function is similar to _responseReceived in RiakNode
    logger.debug('%s receive health check response', this.name);
    this.executeDone();
    this.removeListener('responseReceived', this._boundResponseReceived);
    this._boundResponseReceived = null;
    this._connection.removeListener('close', this._boundConnectionError);
    this._boundConnectionError = null;
    this._connection.on('close', this._connClosed.bind(this));

    var self = this;
    function onError(err) {
        logger.error(err.msg);
        self._connectionError(err.msg);
    }
    function onSuccess() {
        logger.debug('%s health check, emit connected', self.name);
        self._connectedEmitted = true;
        self.emit('connected', self);
    }
    var data = {
        conn: conn,
        command: command,
        code: code,
        decoded: decoded,
        shouldCallback: true
    };
    utils.handleRiakResponse(data, onError, onSuccess);
};

RiakConnection.prototype._receiveStartTls = function(conn, command, code, decoded) {
    // NB: this function is similar to _responseReceived in RiakNode
    logger.debug('%s receive StartTls response', this.name);
    this.executeDone();
    this.removeListener('responseReceived', this._boundResponseReceived);
    this._boundResponseReceived = null;

    var self = this;
    function onError(err) {
        logger.error(err.msg);
        self._connectionError(err.msg);
    }
    function onSuccess() {
        var tls_secure_context = tls.createSecureContext(self.auth);
        var tls_socket_options = {
            isServer: false, // NB: required
            secureContext: tls_secure_context
        };

        self._connection.removeListener('error', self._boundConnectionError);
        self._connection = new tls.TLSSocket(self._connection, tls_socket_options);
        // NB: *must* re-register for data event!
        self._connection.on('data', self._receiveData.bind(self));
        self._connection.on('error', self._boundConnectionError);

        // NB: this is necessary since we have not yet emitted the 'connected' event
        // in which case the execute method would set up the timeout
        // _receiveData clears the timeout as well as the bound method
        self._boundConnectionTimeout = self._connectionTimeout.bind(self);
        self._connection.setTimeout(self.connectionTimeout, self._boundConnectionTimeout);

        var auth_options = {
            user: self.auth.user,
            password: self.auth.password
        };

        // On responseReceived event, move to next sequence in TLS negotiation
        self._boundResponseReceived = self._receiveAuthResp.bind(self);
        self.on('responseReceived', self._boundResponseReceived);

        // Execute AuthReq command
        /*
        if (logger.debug) {
            debugOutputConnectionListeners(self.name, self._connection);
        }
        */
        var command = new AuthReq(auth_options);
        self.execute(command);
    }
    var data = {
        conn: conn,
        command: command,
        code: code,
        decoded: decoded,
        shouldCallback: true
    };
    utils.handleRiakResponse(data, onError, onSuccess);
};

RiakConnection.prototype._receiveAuthResp = function(conn, command, code, decoded) {
    logger.debug('%s receive RpbAuthResp', this.name);
    this.executeDone();
    this.removeListener('responseReceived', this._boundResponseReceived);
    this._boundResponseReceived = null;
    this._connection.removeListener('close', this._boundConnectionError);
    this._boundConnectionError = null;
    this._connection.on('close', this._connClosed.bind(this));

    var self = this;
    function onError(err) {
        logger.error(err.msg);
        self._connectionError(err.msg);
    }
    function onSuccess() {
        logger.debug('%s emit connected, with-auth', self.name);
        self._connectedEmitted = true;
        self.emit('connected', self);
    }
    var data = {
        conn: conn,
        command: command,
        code: code,
        decoded: decoded,
        shouldCallback: true
    };
    utils.handleRiakResponse(data, onError, onSuccess);
};

RiakConnection.prototype._receiveData = function(data) {
    var protobufArray = this._buildProtobufArray(data);
    for (var i = 0; i < protobufArray.length; i++) {
        this._clearSocketTimeout();
        this.emit('responseReceived', this,
            this.command, protobufArray[i].msgCode, protobufArray[i].protobuf);
    }
};

// TODO FUTURE: what does "had_error" really mean?
RiakConnection.prototype._connClosed = function(had_error) {
    this._emitAndClose('connectionClosed');
};

RiakConnection.prototype.close = function() {
    this.closed = true;
    this.executeDone();
    this.removeAllListeners();
    this._closeBuffer();
    if (this._connection) {
        this._connection.end();
        this._connection.removeAllListeners();
        this._connection.on('error', function (err) {
            if (err) {
                logger.error('%s error AFTER close:', this.name, err);
            }
        });
        this._connection = null;
    }
    logger.debug('%s closed', this.name);
};

RiakConnection.prototype.executeDone = function() {
    this.inFlight = false;
    this.command = {
        name: 'no-command'
    };
    this._resetBuffer();
};

// command includes user callback
RiakConnection.prototype.execute = function(command) {
    if (this.inFlight === true) {
        logger.error('%s attempted to run command "%s" on in-use connection',
            this.name, command.name);
        return false;
    }

    this._executeStart(command);

    // write PB to socket
    var message = command.getRiakMessage();

    /*
     * NB: only bind to 'timeout' if 'connected' event has been emitted.
     * Initial connection, health check and starting TLS bind 'timeout'
     * to a handler that will raise 'connectFailed' on timeout
     */
    if (this._connectedEmitted) {
        if (this._boundConnectionTimeout) {
            this._connection.removeListener('timeout', this._boundConnectionTimeout);
        }
        this._boundConnectionTimeout = this._connHandleTimeout.bind(this, command);
        this._connection.setTimeout(this.requestTimeout, this._boundConnectionTimeout);
    }

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
