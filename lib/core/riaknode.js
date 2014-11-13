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
        var conn = new RiakConnection({
            remoteAddress : this.remoteAddress,
            remotePort : this.remotePort,
            connectionTimeout : this.connectionTimeout
        }); 
        
        conn.on('connected', function(conn) {
            self._consecutiveConnectFailures = 0;
            self._currentNumConnections++;
            conn.on('responseReceived', self._responseReceived.bind(self));
            conn.on('connectionClosed', self._connectionClosed.bind(self));
            self._available.push(conn);
        });
        
        conn.on('connectFailed', function(){
           self._consecutiveConnectFailures++; 
        });
        
        conn.connect();
    }
    
    // We actually don't care if the connections have started up yet or not
    this.state = State.RUNNING;
    this.emit('stateChange', this, this.state);
};

RiakNode.prototype.execute = function(command) {
    var conn = this._available.shift();
    var self = this;
    // conn will be undefined if there's no available connections.
    if (!conn) {
        if (this._currentNumConnections < this.maxConnections) {
            
            var conn = new RiakConnection({
                remoteAddress : this.remoteAddress,
                remotePort : this.remotePort,
                connectionTimeout : this.connectionTimeout
            });
            
            conn.on('connected', function(conn) {
                self._currentNumConnections++;
                self._consecutiveConnectFailures = 0;
                conn.on('responseReceived', self._responseReceived.bind(self));
                conn.on('connectionClosed', self._connectionClosed.bind(self));
                conn.execute(command);
            });
            
            conn.on('connectFailed', function(conn, err) {
                self._consecutiveConnectFailures++;
                self.emit('operationFailed', self, command);
            });
            
            conn.connect();
            
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
    conn.inFlight = false;
    conn.resetBuffer();
    this._available.unshift(conn);
    console.log("Number of avail connections: " + this._available.length);
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
    // TODO: track failure for healthcheck / schedule healthcheck?
};

RiakNode.prototype._stateCheck = function(allowedStates) {
    if (allowedStates.indexOf(this.state) === -1) {
        throw "Illegal State; required: " + allowedStates + "current: " + this.state; 
    }
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