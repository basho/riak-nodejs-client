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

var RiakNode = require ('./RiakNode');
var events = require('events');
var logger = require('winston');
var Joi = require('joi');
var util = require('util');

function RiakCluster(options) {
    
    events.EventEmitter.call(this);
    var self = this;
    
    if (options === undefined) {
        options = {};
    }
    
    Joi.validate(options, schema, function(err, options) {
        if (err) {
            throw err;
        }
        
        self.nodes = options.nodes;
        self.executionAttempts = options.executionAttempts;
        self.state = State.CREATED;
    });
    
    this._nodeIndex = 0;
    
}

util.inherits(RiakCluster, events.EventEmitter);

RiakCluster.prototype.start = function() {
    this._stateCheck([State.CREATED]);
    
    for (var i = 0; i < this.nodes.length; i++) {
        this.nodes[i].on('stateChange', this._onNodeStateChange.bind(this));
        this.nodes[i].on('retryCommand', this._onRetryCommand.bind(this));
        this.nodes[i].start();
    }
    
    logger.info("RiakCluster is starting.");
    this.state = State.RUNNING;
    emit('stateChange', this.state);
    
};

RiakCluster.prototype.stop = function() {
    this._stateCheck([State.RUNNING]);
    
    logger.info("RiakCluster is shutting down");
    this.state = State.SHUTTING_DOWN;
    this.emit('stateChange', this, this.state);
    this._shutdown();
    
};

RiakCluster.prototype._shutdown = function () {
    
    var allStopped = true;
    for (var i = 0; i < nodes.length; i++) {
        if (nodes[i].state !== RiakNode.state.SHUTDOWN) {
            allStopped = false;
        }
    }
    
    if (allStopped) {
        this.state = State.SHUTDOWN;
        logger.info("RiakCluster is shut down.");
        emit('stateChange', this.state);
    } else {
        var self = this;
        setTimeout(function() {
            self._shutdown.bind(self);
        }, 1000);
    }
};

RiakCluster.prototype.execute = function(riakCommand) {
    
    // If there's no previous node, set the remaining retries
    if (arguments.length === 1) {
        riakCommand.remainingTries = this.executionAttempts;
    }
    
    var executing = false;
    var startingIndex = this._nodeIndex;
    do {
        var node = this.nodes[this._nodeIndex];
        this._nodeIndex++;
        if (this._nodeIndex === this.nodes.length) {
            this._nodeIndex = 0;
        }
        // don't try the same node twice in a row if we have multiple nodes
        if (this.nodes.length > 1 && arguments.length === 2 && arguements[1] === node) {
            continue;
        }
        
        if (node.state === RiakNode.state.RUNNING) {
            if (node.execute(riakCommand)) {
                executing = true;
                break;
            }
        }
        
    } while (this._nodeIndex !== startingIndex);
    
    if (!executing) {
        riakCommand.onError("No RiakNodes available to execute command.");
    }
    
};

RiakCluster.prototype.addNode = function(node) {
  
  node.on('stateChange', this._onNodeStateChange.bind(this));
  node.on('retryCommand', this._onRetryCommand.bind(this));
  node.start();
  this.nodes.push(node);
};

RiakCluster.prototype.removeNode = function(node) {
    for (var i = 0; i < this.nodes.length; i++) {
        if (node instanceof RiakNode) {
            if (node === this.nodes[i]) {
                this.nodes.splice(i,1);
                return true;
            }
        } else {
            // Hopefully it's a string "addr[:port]"
            var split = node.split(':');
            if (this.nodes[i].remoteAddress === split[0]) {
                if (split.length === 2) {
                    if (this.nodes[i].remotePort === split[1]) {
                        this.nodes.splice(i,1);
                        return true;
                    }
                } else {
                    this.nodes.splice(i,1);
                    return true;
                }
            }
        }
    }
    
    return false;
};



RiakCluster.prototype._onNodeStateChange = function(node, state) {
    emit('nodeStateChange', node, state);
};

RiakCluster.prototype._onRetryCommand = function(command, lastNode) {
    this.execute(command, lastNode);
};

RiakCluster.prototype._stateCheck = function(allowedStates) {
    if (allowedStates.indexOf(this.state) === -1) {
        throw "Illegal State; required: " + allowedStates + "current: " + this.state; 
    }
};


var State = Object.freeze({ CREATED : 0, 
                            RUNNING : 1, 
                            SHUTTING_DOWN : 2, 
                            SHUTDOWN : 3});

var defaultRiakNode = new RiakNode();
var defaultExecutionAttempts = 3;

var schema = Joi.object().keys({
    nodes: Joi.object().default(defaultRiakNode),
    executionAttempts: Joi.number().min(1).default(defaultExecutionAttempts)
    
});

module.exports = RiakCluster;
module.exports.state = State;
