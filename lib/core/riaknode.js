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

function RiakNode(options) {
    
    events.EventEmitter.call(this);
    
    var self = this;
    
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
    
    // We actually don't care if the connections have started up yet or not
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

RiakNode.prototype.stop = function() {
    this._stateCheck([State.RUNNING, State.HEALTH_CHECKING]);
    
    clearInterval(this._expireTimer);
    
    while (this._available.next()) {
        var conn = this._available.removeCurrent();
        this._currentNumConnections--;
        conn.close();
    }
    this._available.resetCursor();
    
    if (this._currentNumConnections === 0) {
        this.state = State.SHUTDOWN;
    } else {
        // There's still connections out and being used
        this.state = State.SHUTTING_DOWN;
    }
    
    this.emit('stateChange', this, this.state);
};

RiakNode.prototype.execute = function(command) {
    this._stateCheck([State.RUNNING, State.HEALTH_CHECKING]);
    var conn = this._available.shift();
    var self = this;
    // conn will be undefined if there's no available connections.
    if (!conn) {
        if (this._currentNumConnections < this.maxConnections) {
            
            this._createNewConnection(function(newConn) { 
                newConn.execute(command);
            }, function(err) {
                if (self._consecutiveConnectFailures > 5 && 
                        self.state === State.RUNNING) {
                    self.state = State.HEALTH_CHECKING;
                    setTimeout(self._healthCheck.bind(self), 1000);
                }
                command.remainingTries--;
                if (command.remainingTries) {
                    self.emit('retryCommand', command);
                } else {
                    command.onError(err);
                }
            });
            
            return true;
            
        } else {
            console.log('Riaknode: all connections in use and at max');
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
        console.log('RiakNode: recevied RpbErrorResp for command; ' + decoded.getErrmsg().toString());
        if (command.remainingTries) {
            this.emit('retryCommand', command);
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
            this.emit('retryCommand', command);
        } else {
            command.onError(msg);
        }
    } else {
        var done = command.onSuccess(decoded);
        if (done) {
            console.log('Riaknode: command complete');
            this._returnConnectionToPool(conn);
        }
    }
    
};

RiakNode.prototype._returnConnectionToPool = function(conn) {
    
    if (this.state !== State.SHUTTING_DOWN) {
        conn.inFlight = false;
        conn.resetBuffer();
        this._available.unshift(conn);
        console.log("Number of avail connections: " + this._available.length);
    } else {
        this._currentNumConnections--;
        conn.close();
        if (this._currentNumConnections === 0) {
            this.state = State.SHUTDOWN;
            this.emit('stateChange', this, this.state);
        }
    }
    
};

RiakNode.prototype._connectionClosed = function(conn) {
    this._currentNumConnections--;
    // See if a command was being handled
    console.log("RiakNode: Connection closed; inFlight: " + conn.inFlight);
    if (conn.inFlight) {
        var command = conn.command;
        command.remainingTries--;
        if (command.remainingTries) {
            this.emit('retryCommand', command);
        } else {
            command.callback(msg, null);
        }
    }
    
    if (this.state === State.SHUTTING_DOWN && this._currentNumConnections === 0) {
        this.state = State.SHUTDOWN;
        this.emit('stateChange', this, this.state);
    }
    
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

module.exports = RiakNode;