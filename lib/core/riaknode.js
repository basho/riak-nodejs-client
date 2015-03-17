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

var Joi = require('joi');
var events = require('events');
var util = require('util');
var RiakConnection = require('./riakconnection');
var LinkedList = require('linkedlist');
var RiakProtobuf = require('../protobuf/riakprotobuf');
var logger = require('winston');

/**
 * Provides the RiakNode class and its Builder.
 * @module RiakNode
 */

/**
 * A class that represents a node in a Riak cluster.  
 * 
 * Instances of this class maintain connections to and execute commands on
 * a Riak node in a Riak cluster.
 * 
 * __options__ is an object with the following defaults:
 * 
 *     { 
 *       remoteAdddress: '127.0.0.1',
 *       remotePort: 8087,
 *       maxConnections: 10000,
 *       minConnections: 1,
 *       idleTimeout: 3000,
 *       connectionTimeout: 0
 *     }
 *     
 * As a convenience a builder method is provided;
 * 
 *     var RiakNode = require('./lib/core/riaknode');
 *     
 *     var newNode = new RiakNode.Builder().withMinConnections(10).build(); 
 * 
 * 
 * @param {Object} options - the options for this RiakNode.
 * 
 */
function RiakNode(options) {
    
    events.EventEmitter.call(this);
    
    var self = this;
    
    if (options === undefined) {
        options = {};
    }
    
    Joi.validate(options, schema, function(err, options) {
       
        if (err) {
            throw err;
        }
        
        self.remoteAddress = options.remoteAddress;
        self.remotePort = options.remotePort;
        self.minConnections = options.minConnections;
        self.maxConnections = options.maxConnections;
        self.idleTimeout = options.idleTimeout;
        self.connectionTimeout = options.connectionTimeout;
        self.state = State.CREATED;
    });
    
    this._available = new LinkedList();
    this._consecutiveConnectFailures = 0;
    this._currentNumConnections = 0;
    
}

util.inherits(RiakNode, events.EventEmitter);

/**
 * Start this RiakNode.
 * @method start
 */
RiakNode.prototype.start = function() {
    this._stateCheck([State.CREATED]);
    var self = this;
    
    // Fire up connection pool    
    for (i = 0; i < this.minConnections; i++) {
        
        this._createNewConnection(this._returnConnectionToPool.bind(this), 
                                    function(){});
    }
    
    this._expireTimer = setInterval(function() {
       self._expireIdleConnections.bind(self);
    }, 30000);
    
    this.state = State.RUNNING;
    this.emit('stateChange', this, this.state);
};

RiakNode.prototype._createNewConnection = function(postConnectFunc, postFailFunc) {
  
    var conn = new RiakConnection({
        remoteAddress : this.remoteAddress,
        remotePort : this.remotePort,
        connectionTimeout : this.connectionTimeout
    }); 
    
    var self = this;
    
    conn.on('connected', function(conn) {
        self._consecutiveConnectFailures = 0;
        self._currentNumConnections++;
        conn.on('responseReceived', self._responseReceived.bind(self));
        conn.on('connectionClosed', self._connectionClosed.bind(self));
        postConnectFunc(conn);
    });
        
    conn.on('connectFailed', function(conn, err){
       self._consecutiveConnectFailures++;
       postFailFunc(err);
    });

    conn.connect();

};

/**
 * Stop this RiakNode.
 * @method stop
 */
RiakNode.prototype.stop = function() {
    this._stateCheck([State.RUNNING, State.HEALTH_CHECKING]);
    
    clearInterval(this._expireTimer);
    this.state = State.SHUTTING_DOWN;
    this.emit('stateChange', this, this.state);
    this._shutdown();
};

RiakNode.prototype._shutdown = function() {
    
    while (this._available.next()) {
        var conn = this._available.removeCurrent();
        this._currentNumConnections--;
        conn.close();
    }
    this._available.resetCursor();
    
    if (this._currentNumConnections === 0) {
        this.state = State.SHUTDOWN;
        this.emit('stateChange', this, this.state);
    } else {
        var self = this;
        this._shutdownTimer = setTimeout(function() {
            self._shutdown.bind(self);
        }, 1000);
    }
};

/**
 * Execute a command on this RiakNode.
 * @param {Object} command - a command to execute.
 * @return {Boolean} - if this RiakNode accepted the command for execution.
 */
RiakNode.prototype.execute = function(command) {
    this._stateCheck([State.RUNNING, State.HEALTH_CHECKING]);
    var conn = this._available.shift();
    var self = this;
    // conn will be undefined if there's no available connections.
    if (!conn) {
        if (this._currentNumConnections < this.maxConnections) {
            
            this._createNewConnection(function(newConn) { 
                self._consecutiveConnectFailures = 0;
                newConn.execute(command);
            }, function(err) {
                if (self._consecutiveConnectFailures > 5 && 
                        self.state === State.RUNNING) {
                    self.state = State.HEALTH_CHECKING;
                    setTimeout(self._healthCheck.bind(self), 1000);
                }
                command.remainingTries--;
                if (command.remainingTries) {
                    self.emit('retryCommand', command, self);
                } else {
                    command.onError(err);
                }
            });
            
            return true;
            
        } else {
            logger.info('Riaknode (%s): all connections in use and at max', this.remoteAddress);
            return false;
        }
    } else {
        conn.execute(command);
        return true;
    }
        
};

RiakNode.prototype._responseReceived = function(conn, command, code, decoded) {
    
    command.remainingTries--;
    if (code === RiakProtobuf.getCodeFor('RpbErrorResp')) {
        this._returnConnectionToPool(conn);
        logger.info('RiakNode (%s): recevied RpbErrorResp for command; %s', this.remoteAddress, decoded.getErrmsg().toString());
        if (command.remainingTries) {
            this.emit('retryCommand', command, this);
        } else {
            command.onRiakError(decoded);
        }
    } else if (code !== command.getExpectedResponseCode()) {
        // TODO: Nuke connetion here?
        this._returnConnectionToPool(conn);
        var msg = 'Riaknode received wrong reponse; expected ' +
                        command.getExpectedResponseCode() +
                        ' received ' + code;
        if (command.remainingTries) {
            this.emit('retryCommand', command, this);
        } else {
            command.onError(msg);
        }
    } else {
        var done = command.onSuccess(decoded);
        if (done) {
            logger.info('Riaknode (%s): command complete', this.remoteAddress);
            this._returnConnectionToPool(conn);
        }
    }
    
};

RiakNode.prototype._returnConnectionToPool = function(conn) {
    
    if (this.state !== State.SHUTTING_DOWN) {
        conn.inFlight = false;
        conn.resetBuffer();
        this._available.unshift(conn);
        logger.info("RiakNode (%s); Number of avail connections: %d", this.remoteAddress, this._available.length);
    } else {
        this._currentNumConnections--;
        conn.close();
    }
    
};

RiakNode.prototype._connectionClosed = function(conn) {
    this._currentNumConnections--;
    // See if a command was being handled
    logger.log("RiakNode (%s): Connection closed; inFlight: %d", this.remoteAddress, conn.inFlight);
    if (conn.inFlight) {
        var command = conn.command;
        command.remainingTries--;
        if (command.remainingTries) {
            this.emit('retryCommand', command, this);
        } else {
            command.onError("Connection closed while executing command");
        }
    }
    
    if (this.state !== State.SHUTTING_DOWN) {
        // PB connections don't time out. If one disconnects it's highly likely
        // the node went down or there's a network issue
        if (this.state !== State.HEALTH_CHECKING) {
            this.state = State.HEALTH_CHECKING;
            var self = this;
            setTimeout(function() {
                self._healthCheck.bind(self);
            }, 1000);

            this.emit('stateChange', this, this.state);
        }
    }
};

RiakNode.prototype._stateCheck = function(allowedStates) {
    if (allowedStates.indexOf(this.state) === -1) {
        throw "Illegal State; required: " + allowedStates + "current: " + this.state; 
    }
};

RiakNode.prototype._healthCheck = function() {
    
    // TODO: Add ping op instead of just connecting
    var self = this;
    
    this._createNewConnection(function(newConn) {
        self._returnConnectionToPool(newConn);
        self.state = State.RUNNING;
        self.emit('stateChange', self, self.state);
    }, function() {
        setTimeout(self._healthCheck.bind(self), 30000);
    });
};

RiakNode.prototype._expireIdleConnections = function() {
    var now = Date.now();
    while (this._available.next() && this._currentNumConnections > this.minConnections) {
        if (now - this._available.current >= this.idleTimeout) {
            var conn = this._available.removeCurrent();
            this._currentNumConnections--;
            conn.close();
        }
    }
    this._available.resetCursor();
};

var State = Object.freeze({ CREATED : 0, 
                            RUNNING : 1, 
                            HEALTH_CHECKING : 2, 
                            SHUTTING_DOWN : 3, 
                            SHUTDOWN : 4});

var defaultRemoteAddress = "127.0.0.1";
var defaultRemotePort = 8087;
var defaultMinConnections = 1;
var defaultMaxConnections = 10000; // magic number but close enough 
var defaultIdleTimeout = 3000;
var defaultConnectionTimeout = 0;

var schema = Joi.object().keys({
    remoteAddress: Joi.string().default(defaultRemoteAddress),
    remotePort: Joi.number().min(1).default(defaultRemotePort),
    minConnections: Joi.number().min(0).default(defaultMinConnections),
    maxConnections: Joi.number().min(0).default(defaultMaxConnections),
    idleTimeout: Joi.number().min(1000).default(defaultIdleTimeout),
    connectionTimeout: Joi.number().min(1).default(defaultConnectionTimeout)
});

/**
 * This event is fired whenever the state of the RiakNode changes.
 * @event stateChange
 * @param {Object} node - the RiakNode object whose state changed
 * @param {Number} state - the RiakNode.state
 */
var EVT_SC = 'stateChange';

/**
 * This event is fired whenever a command fails and needs to be retried.
 * @event retryCommand
 * @param {Object} command - the command to retry
 * @param {RiakNode} node - this RiakNode 
 */
var EVT_RC = 'retryCommand';

/**
 * A Builder for constructing RiakNode instances.
 * 
 * Rather than having to manually construct the __options__ and instantiating
 * a RiakNode directly, this builder may be used.
 * 
 *      var RiakNode = require('./lib/core/RiakNode');
 *      var riakNode = new RiakNode.Builder().withRemotePort(9999).build();
 *       
 * @namespace RiakNode
 * @class Builder
 * @constructor
 */
function Builder() {}
    
Builder.prototype = {
  
    /**
     * Set the remote address for the RiakNode.
     * @param {String} address - IP or hostanme of the Riak node (__default:__ 127.0.0.1)
     * @return {Builder.prototype}
     */
    withRemoteAddress : function(address) {
        this.remoteAddress = address;
        return this;
    },
    
    /**
     * Set the remote port for this RiakNode.
     * @param {Number} port - remote port of the Riak node (__default:__ 8087)
     * @return {Builder.prototype}
     */
    withRemotePort : function(port) {
        this.remotePort = port;
        return this;
    },
    
    /**
     * Set the minimum number of active connections to maintain.
     * These connections are exempt from the idle timeout.
     * @param {Number} minConnections - number of connections to maintain (__default:__ 1)
     * @return {Builder.prototype}
     */
    withMinConnections : function(minConnections) {
        this.minConnections = minConnections;
        return this;
    },
    
    /**
     * Set the maximum number of connections allowed.
     * @param {Number} maxConnections - maximum number of connections to allow (__default:__ 10000)
     * @return {Builder.prototype}
     */
    withMaxConnections : function(maxConnections) {
        this.maxConnections = maxConnections;
        return this;
    },
    
    /**
     * Set the idle timeout used to reap inactive connections.
     * Any connection that has been idle for this amount of time
     * becomes eligible to be closed and discarded excluding the number 
     * set via __withMinConnections()__.
     * @param {Number} idleTimeout - the timeout in milliseconds (__defualt:__ 3000)
     * @return {Builder.prototype}
     */
    withIdleTimeout : function(idleTimeout) {
        this.idleTimeout = idleTimeout;
        return this;
    },
    
    /**
     * Set the connection timeout used when making new connections.
     * @param {Number} connectionTimeout - timeout in milliseconds (__default:__ 0).
     * @return {Builder.prototype}
     */
    withConnectionTimeout : function(connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    },
    
    /**
     * Builds a RiakNode instance.
     * @return {RiakNode}
     */
    build : function() {
        return new RiakNode(this);
    }
    
};

/**
 * Static factory for constructing a set of RiakNodes.
 * 
 * To create a set of RiakNodes with the same options:
 * 
 *      var options = new RiakNode.Builder().withMinConnections(10);  
 *      var nodes = RiakNode.buildNodes(['192.168.1.1', '192.168.1.2'], options); 
 *      
 * __options__ can be manually constructed or an instance of the Builder.
 * 
 * @param {String[]} addresses - an array of IP|hostname[:port] 
 * @param {Object} [options] - the options to use for all RiakNodes.
 * @return {Array/buildNodes.riakNodes}
 */
var buildNodes = function(addresses, options) {
    var riakNodes = [];
    
    if (options === undefined) {
        options = {};
    }
    
    for (var i =0; i < addresses.length; i++) {
        var split = addresses[i].split(':');
        options.remoteAddress = split[0];
        if (split.length === 2) {
            options.remotePort = split[1];
        }
        riakNodes.push(new RiakNode(options));
    }
    
    return riakNodes;
};

module.exports = RiakNode;
module.exports.buildNodes = buildNodes;
module.exports.Builder = Builder;
module.exports.State = State;
