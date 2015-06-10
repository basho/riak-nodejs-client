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
var DefaultNodeManager = require('./defaultnodemanager');
var events = require('events');
var logger = require('winston');
var Joi = require('joi');
var util = require('util');
var LinkedList = require('linkedlist');

/**
 * @module Core
 */

/**
 * Provides the RiakCluster class and its Builder.
 * 
 * Instances of this class maintain a set of {{#crossLink "RiakNode"}}{{/crossLink}} objects and 
 * executes commands on them.
 * 
 * __options__ is an object with the following defaults:
 * 
 *     {
 *       nodes: [defaultRiakNode],
 *       executionAttempts: 3,
 *       nodeManager: DefaultNodeManager,
 *       queueCommands: false,
 *       queueMaxDepth: unlimited
 *     }
 *     
 * The __defaultRiakNode__ is a RiakNode connected to 127.0.0.1:8087 
 * 
 * As a convenience a builder class is provided. 
 * 
 *     var nodeTemplate = new RiakNode.Builder().withMinConnections(10);
 *     var nodeAddys = ['192.168.1.1', '192.168.1.2'];
 *     var arrayOfNodes = RiakNode.buildNodes(nodeAddys, nodeTemplate);
 *     var myCluster = new RiakCluster.Builder().withRiakNodes(arrayOfNodes).build();
 *     
 * See {{#crossLink "RiakCluster.Builder"}}RiakCluster.Builder{{/crossLink}}
 *
 * @class RiakCluster
 * @constructor
 * @param {Object} options - the options to use.
 * @param {RiakNode[]} options.nodes An array of (unstarted) {{#crossLink "RiakNode"}}{{/crossLink}} objects.
 * @param {Number} [options.executionAttempts=3] Number of times to retry commands on failure.
 * @param {Object} [options.nodeManager=DefaultNodeManager] Set the NodeManager for this cluster.
 * @param {Boolean} [options.queueCommands=false] Set whether to queue commands or not if no RiakNodes are available.
 * @param {Number} [options.queueMaxDepth=unlimited] The maximum number of commands to queue if queueCommands is set. Default is unlimited.
 * 
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
        self.nodeManager = options.nodeManager;
        self.queueCommands = options.queueCommands;
        self.queueMaxDepth = options.queueMaxDepth;
    });

    this._commandQueue = new LinkedList();

}

util.inherits(RiakCluster, events.EventEmitter);

/**
 * Start this RiakCluster
 * @method start
 */
RiakCluster.prototype.start = function() {

    if (this._state === State.RUNNING) {
        logger.warning('[RiakCluster] cluster already running.');
    } else {
        this._stateCheck([State.CREATED]);
        logger.debug('[RiakCluster] starting.');

        for (var i = 0; i < this.nodes.length; i++) {
            this._startNode(this.nodes[i]);
        }

        this.state = State.RUNNING;
        logger.debug('[RiakCluster] cluster started.');
        this.emit(EVT_SC, this.state);
    }

};

/**
 * Stop this RiakCluster
 * @method stop
 */
RiakCluster.prototype.stop = function() {

    this._stateCheck([State.RUNNING, State.QUEUEING]);

    logger.debug('RiakCluster is shutting down');

    this.state = State.SHUTTING_DOWN;
    this.emit(EVT_SC, this.state);

    for (var i = 0; i < this.nodes.length; i++) {
        this.nodes[i].stop();
    }

    this._shutdown();

};

RiakCluster.prototype._startNode = function (node) {
    node.on(EVT_SC, this._onNodeStateChange.bind(this));
    node.on(EVT_RC, this._onRetryCommand.bind(this));
    node.start();
};

RiakCluster.prototype._shutdown = function () {

    logger.debug('[RiakCluster] checking to see if nodes are shut down.'); 

    var allStopped = true;
    for (var i = 0; i < this.nodes.length; i++) {
        if (this.nodes[i].state !== RiakNode.State.SHUTDOWN) {
            allStopped = false;
        }
    }

    if (allStopped) {
        this.state = State.SHUTDOWN;
        logger.debug('[RiakCluster] cluster shut down.');
        if (this._commandQueue.length) {
            logger.warn('[RiakCluster] There were %d commands in the queue at shutdown', 
                this._commandQueue.length);
        }
        this.emit(EVT_SC, this.state);
    } else {
        logger.debug('[RiakCluster] nodes still running.');
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
 * @param {RiakNode} [previous] the previos node this command was attempted on
 */
RiakCluster.prototype.execute = function(riakCommand, previous) {

    // If there's no previous node, set the remaining retries
    if (arguments.length === 1) {
        riakCommand.remainingTries = this.executionAttempts;
    }

    var executing = false;
    if (this._commandQueue.length === 0) {
        executing = this.nodeManager.executeOnNode(this.nodes, riakCommand, previous);
    }

    if (!executing) {
        if (this.queueCommands) {
            if (this.queueMaxDepth && (this._commandQueue.length >= this.queueMaxDepth)) {
                riakCommand.onError("No RiakNodes available and command queue at maxDepth");
            } else {
                this._commandQueue.push(riakCommand);
                if (this.state === State.RUNNING) {
                    this.state = State.QUEUEING;

                    // TODO: should this timeout be configurable, or based on an
                    // average command execution time?
                    setTimeout(this._submitFromQueue.bind(this), 500);
                    logger.info('[RiakCluster] queueing commands.');
                    this.emit(EVT_SC, this.state);
                }
            }
        } else {
            riakCommand.onError('No RiakNodes available to execute command.');
        }
    }

};

RiakCluster.prototype._submitFromQueue = function() {

    if (this.state < State.SHUTTING_DOWN) {
        logger.debug('[RiakCluster] submitFromQueue %d.', this._commandQueue.length);
        var command;
        var executing;
        while (this._commandQueue.length) {
            command = this._commandQueue.shift();
            executing = this.nodeManager.executeOnNode(this.nodes, command);
            if (!executing) {
                this._commandQueue.unshift(command);
                // TODO: should this timeout be configurable, or based on an
                // average command execution time?
                setTimeout(this._submitFromQueue.bind(this), 500);
                break;
            }
        }

        if (!this._commandQueue.length) {
            this.state = State.RUNNING;
            logger.debug('[RiakCluster] cleared command queue.');
            this.emit(EVT_SC, this.state);
        }
    }

};

/**
 * Add a RiakNode ot this cluster.
 * @method addNode
 * @param {RiakNode} node the (unstarted) RiakNode to add.
 */
RiakCluster.prototype.addNode = function(node) {
    this._startNode(node);
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
                    if (this.nodes[i].remotePort === Number(split[1])) {
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
    this.emit(EVT_NSC, node, state);
};

RiakCluster.prototype._onRetryCommand = function(command, lastNode) {
    this.execute(command, lastNode);
};

RiakCluster.prototype._stateCheck = function(allowedStates) {
    if (allowedStates.indexOf(this.state) === -1) {
        throw 'RiakCluster: Illegal State; required: ' + allowedStates + ' current: ' + this.state; 
    }
};

/**
 * The state of this cluster.
 * 
 * If listeneing for stateChange events, a numeric value will be sent that
 * can be compared to:
 * 
 *     RiakCluster.State.CREATED
 *     RiakCluster.State.RUNNING
 *     RiakCluster.State.SHUTTING_DOWN
 *     RiakCluster.State.SHUTDOWN
 * 
 * See: {{#crossLink "RiakCluster/stateChange:event"}}stateChange{{/crossLink}}
 * 
 * @property State
 * @type {Object}
 * @static
 * @final
 */
var State = Object.freeze({ CREATED : 0, 
                            RUNNING : 1,
                            QUEUEING: 2, 
                            SHUTTING_DOWN : 3, 
                            SHUTDOWN : 4});

var defaultRiakNode = new RiakNode();
var defaultExecutionAttempts = 3;

function createDefaultNodeManager() {
    return new DefaultNodeManager();
}

var schema = Joi.object().keys({
    nodes: Joi.array().min(1).default([defaultRiakNode]),
    executionAttempts: Joi.number().min(1).default(defaultExecutionAttempts),
    nodeManager: Joi.object()
        .default(createDefaultNodeManager, 'default is a new instance of DefaultNodeManager'),
    queueCommands: Joi.boolean().default(false),
    queueMaxDepth: Joi.number().default(0)
});

/**
 * This event is fired when the state of the RiakCluster changes.
 * See: {{#crossLink "RiakCluster/State:property"}}RiakCluster.State{{/crossLink}}
 * @event stateChange
 * @param {Number} state - the new {{#crossLink "RiakCluster/State:property"}}RiakCluster.State{{/crossLink}}
 */
var EVT_SC = 'stateChange';

/**
 * This event is fired when the state of any RiakNode in the cluster changes.
 * @event nodeStateChange
 * @param {Object} node - the {{#crossLink "RiakNode"}}{{/crossLink}} object whose state changed
 * @param {Number} state - the {{#crossLink "RiakNode/State:property"}}RiakNode.State{{/crossLink}}
 */
var EVT_NSC = 'nodeStateChange';

/**
 * This event is fired whenever a command fails on a RiakNode and needs to be retried.
 * RiakCluster is a listener.
 */
var EVT_RC = 'retryCommand';

/**
 * A Builder for constructing RiakCluster instances.
 * 
 * Rather than having to manually construct the __options__ and instantiating
 * a RiakCluster directly, this builder may be used.
 * 
 * var newCluster = new RiakCluster.Builder().withRiakNodes([node1, node2]).build();
 * 
 * @class RiakCluster.Builder
 * @constructor
 */
function Builder() {
    this.nodes = [];
}

Builder.prototype = {
    /**
     * The RiakNodes to use.
     * @method withRiakNodes
     * @param {RiakNode[]} nodes array of (unstarted) {{#crossLink "RiakNode"}}{{/crossLink}} instances.
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
     * Set the NodeManager for this cluster.
     * 
     * If not provided the {{#crossLink "DefaultNodeManager"}}{{/crossLink}} is
     * used.
     * @method withNodeManager
     * @param {NodeManager} nodeManager the node manager used to select nodes.
     * @chainable
     */
    withNodeManager : function(nodeManager) {
        this.nodeManager = nodeManager;
        return this;
    },
    /**
     * Set whether to queue commands or not if no RiakNodes are available.
     * 
     * If all nodes are down (health checking) or maxConnections are in use on
     * all nodes, the default behavior is to fail commands when submitted. 
     * 
     * Setting this option causes the the RiakCluster to queue additional commands
     * (FIFO) then send them when nodes/connections become available. 
     * 
     * If maxDepth is supplied the queue is bounded and additional commands 
     * attempting to be queued will be failed. The default is an unbounded queue.
     * 
     * @method withQueueCommands
     * @param {Number} [maxDepth=unlimited] the maximum number of commands to queue. Default is unlimited.
     * @chainable
     */
    withQueueCommands : function(maxDepth) {
        this.queueCommands = true;
        this.queueMaxDepth = maxDepth;
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
