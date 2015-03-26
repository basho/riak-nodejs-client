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

var RiakNode = require ('./riaknode');
var events = require('events');
var logger = require('winston');
var Joi = require('joi');
var util = require('util');

/**
 * Provides the RiakCluster class and its Builder.
 * @module RiakCluster
 */

/**
 * A Class the represents a Riak cluster.
 * 
 * Instances of this class maintain a set of RiakNode objects and 
 * execute commands on them. 
 * 
 * __options__ is an object with the following defaults:
 * 
 *     {
 *       nodes: [defaultRiakNode],
 *       executionAttempts: 3
 *     }
 *     
 * The __defaultRiakNode__ is a RiakNode connected to 127.0.0.1:8087 
 * 
 * As a convenience a builder class is provided. 
 * 
 *     var nodeTemplate = new RiakNode.Builder().withMinConnections(10);
 *     var nodeAddys = ['192.168.1.1', '192.168.1.2'];
 *     var arrayOfNodes = RiakNode.buildNodes(nodeAddys, nodeTemplate);
 *     var myCluster = new RiakCluster.builder().withNodes(arrayOfNodes).build();
 *
 * @class RiakCluster
 * @constructor
 * @param {type} options - the options to use.
 */
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

/**
 * Start this RiakCluster
 * @method start
 */
RiakCluster.prototype.start = function() {
    this._stateCheck([State.CREATED]);
    logger.info('RiakCluster is starting.');
    
    for (var i = 0; i < this.nodes.length; i++) {
        this.nodes[i].on('stateChange', this._onNodeStateChange.bind(this));
        this.nodes[i].on('retryCommand', this._onRetryCommand.bind(this));
        this.nodes[i].start();
    }
    
    this.state = State.RUNNING;
	logger.info('RiakCluster started.');
    this.emit('stateChange', this.state);
    
};

/**
 * Stop this RiakCluster
 * @method stop
 */
RiakCluster.prototype.stop = function() {
    this._stateCheck([State.RUNNING]);
    
    logger.info('RiakCluster is shutting down');
    this.state = State.SHUTTING_DOWN;
    this.emit('stateChange', this, this.state);
    for (var i = 0; i < this.nodes.length; i++) {
        this.nodes[i].stop();
    }
	this._shutdown();
    
};

RiakCluster.prototype._shutdown = function () {
    logger.info('Checking to see if nodes are shut down.'); 
    var allStopped = true;
    for (var i = 0; i < this.nodes.length; i++) {
        if (this.nodes[i].state !== RiakNode.State.SHUTDOWN) {
            allStopped = false;
        }
    }
    
    if (allStopped) {
        this.state = State.SHUTDOWN;
        logger.info('RiakCluster is shut down.');
        this.emit('stateChange', this.state);
    } else {
        logger.info('RiakNodes still running.');
		var self = this;
        setTimeout(function() {
            self._shutdown();
        }, 1000);
    }
};

/**
 * Execute a command on this RiakCluster.
 * 
 * Selects a RiakNode from the cluster and executes the command on it.
 * @method execute
 * @param {Object} riakCommand - the command to execute.
 */
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
        if (this.nodes.length > 1 && arguments.length === 2 && arguments[1] === node) {
            continue;
        }
        
        if (node.state === RiakNode.State.RUNNING) {
            if (node.execute(riakCommand)) {
                executing = true;
                break;
            }
        }
        
    } while (this._nodeIndex !== startingIndex);
    
    if (!executing) {
        riakCommand.onError('No RiakNodes available to execute command.');
    }
    
};

/**
 * Add a RiakNode ot this cluster.
 * @method addNode
 * @param {RiakNode} node the (unstarted) RiakNode to add.
 */
RiakCluster.prototype.addNode = function(node) {
  
  node.on('stateChange', this._onNodeStateChange.bind(this));
  node.on('retryCommand', this._onRetryCommand.bind(this));
  node.start();
  this.nodes.push(node);
};

/**
 * Remove a RiakNode from this cluster.
 * The node being removed will also be stopped.
 * @method removeNode
 * @param {RiakNode|String} node - the node to remove. May be supplied as a RiakNode instance or IP|hostname[:port]
 * @return {Boolean} - true if the node was removed.
 */
RiakCluster.prototype.removeNode = function(node) {
    for (var i = 0; i < this.nodes.length; i++) {
        if (node instanceof RiakNode) {
            if (node === this.nodes[i]) {
                this.nodes.splice(i,1);
                node.stop();
                return true;
            }
        } else {
            // Hopefully it's a string "addr[:port]"
            var split = node.split(':');
            if (this.nodes[i].remoteAddress === split[0]) {
                var removed;
                if (split.length === 2) {
                    if (this.nodes[i].remotePort === split[1]) {
                        removed = this.nodes.splice(i,1);
                        removed[0].stop();
                        return true;
                    }
                } else {
                    removed = this.nodes.splice(i,1);
                    removed[0].stop();
                    return true;
                }
            }
        }
    }
    
    return false;
};



RiakCluster.prototype._onNodeStateChange = function(node, state) {
    this.emit('nodeStateChange', node, state);
};

RiakCluster.prototype._onRetryCommand = function(command, lastNode) {
    this.execute(command, lastNode);
};

RiakCluster.prototype._stateCheck = function(allowedStates) {
    if (allowedStates.indexOf(this.state) === -1) {
        throw 'Illegal State; required: ' + allowedStates + 'current: ' + this.state; 
    }
};


var State = Object.freeze({ CREATED : 0, 
                            RUNNING : 1, 
                            SHUTTING_DOWN : 2, 
                            SHUTDOWN : 3});

var defaultRiakNode = new RiakNode();
var defaultExecutionAttempts = 3;

var schema = Joi.object().keys({
    nodes: Joi.array().min(1).default([defaultRiakNode]),
    executionAttempts: Joi.number().min(1).default(defaultExecutionAttempts)
    
});

/**
 * This event is fired when the state of the RiakCluster changes.
 * @event stateChange
 * @param {Number} state - the new RiakCluster.State
 */
var EVT_SC = 'stateChange';

/**
 * This event is fired when the state of any RiakNode in the cluster changes.
 * @event nodeStateChange
 * @param {Object} node - the RiakNode object whose state changed
 * @param {Number} state - the RiakNode.state
 */
var EVT_NSC = 'nodeStateChange';

/**
 * A Builder for constructing RiakCluster instances.
 * 
 * Rather than having to manually construct the __options__ and instantiating
 * a RiakCluster directly, this builder may be used.
 * 
 * var RiakCluster = require('./lib/core/RiakCluster');
 * 
 * var newCluster = new RiakCluster.Builder().withRiakNodes([node1, node2]).build();
 * 
 * @namespace RiakCluster
 * @class Builder
 * @constructor
 */
function Builder() {
    this.nodes = [];
}


Builder.prototype = {
    
    /**
     * The RiakNodes to use.
     * @method withRiakNodes
     * @param {RiakNode[]} nodes array of (unstarted) RiakNode instances.
     * @return {RiakCluster.Builder}
     */
    withRiakNodes : function(nodes) {
        for (var i = 0; i < nodes.length; i++) {
        
            if (nodes[i] instanceof RiakNode) {
                this.nodes.push(nodes[i]); 
            }
        }
        return this;
    },
    
    /**
     * Set the number of times a command will be attempted.
     * In the case of a command failing for any reason, it will be retied 
     * on a different node. 
     * @method withExecutionAttempts
     * @param {Number} numAttempts - the number of times to attempt a command (__default:__ 3)
     * @return {RiakCluster.Builder}
     */
    withExecutionAttmpts : function(numAttempts) {
        this.executionAttempts = numAttempts;
        return this;
    },
    
    /**
     * Builds a RiakCluster instance.
     * @method build
     * @return {RiakCluster}
     */
    build : function() {
        return new RiakCluster(this);
    }
    
};

module.exports = RiakCluster;
module.exports.Builder = Builder;
module.exports.State = State;
