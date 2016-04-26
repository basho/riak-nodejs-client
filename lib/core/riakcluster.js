'use strict';

var async = require('async');
var events = require('events');
var Joi = require('joi');
var logger = require('winston');
var util = require('util');

var RiakNode = require ('./riaknode');
var RoundRobinNodeManager = require('./roundrobinnodemanager');
var utils = require('./utils');

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
 *       nodeManager: RoundRobinNodeManager,
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
 * @param {Object} [options.nodeManager=RoundRobinNodeManager] Set the NodeManager for this cluster.
 * @param {Boolean} [options.queueCommands=false] Set whether to queue commands or not if no RiakNodes are available.
 * @param {Number} [options.queueMaxDepth=unlimited] The maximum number of commands to queue if queueCommands is set. Default is unlimited.
 * @param {Number} [options.queueSubmitInterval=500] The duration in milliseconds between queue submission attempts. Default is 500.
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
        self.queueSubmitInterval = options.queueSubmitInterval;
    });

    this._commandQueue = [];

    // Note: useful for debugging event issues
    // this.setMaxListeners(1);

    this._stateCheck = function(allowedStates) {
        return utils.stateCheck('[RiakCluster]', this.state,
            allowedStates, stateNames);
    };
}

util.inherits(RiakCluster, events.EventEmitter);

/**
 * Start this RiakCluster
 * @method start
 * @param {Function} [callback] - a callback for when cluster is started.
 * @param {Object} [callback.err] - will be set to an error if one occurred.
 * @param {Object} [callback.cluster] - will be set to the cluster object.
 */
RiakCluster.prototype.start = function(callback) {
    if (this._state === State.RUNNING) {
        logger.warning('[RiakCluster] cluster already running.');
    } else {
        this._stateCheck([State.CREATED]);
        logger.debug('[RiakCluster] starting.');

        var funcs = [];
        for (var i = 0; i < this.nodes.length; i++) {
            funcs.push(makeStartNodeFunc(this, this.nodes[i]));
        }

        var self = this;
        async.parallel(funcs, function (err, rslts) {
            self.state = State.RUNNING;
            logger.debug('[RiakCluster] cluster started.');
            self.emit(EVT_SC, self.state);
            if (callback) {
                callback(err, self);
            }
        });
    }
};

function makeStartNodeFunc(cluster, node) {
    var f = function (async_cb) {
        cluster._startNode(node, async_cb);
    };
    return f;
}

/**
 * Stop this RiakCluster
 * @method stop
 * @param {Function} callback - called when cluster completely stopped.
 * @param {Object} [callback.err] - set to an error if one occurrs during start.
 * @param {Object} [callback.state] - the state of the cluster at shutdown.
 */
RiakCluster.prototype.stop = function(callback) {
    this._stateCheck([State.RUNNING, State.QUEUEING]);
    logger.debug('[RiakCluster] shutting down');
    this.state = State.SHUTTING_DOWN;
    this.emit(EVT_SC, this.state);
    var funcs = [];
    this.nodes.forEach(function (node) {
        funcs.push(makeStopNodeFunc(node));
    });
    var self = this;
    async.parallel(funcs, function (err, rslts) {
        if (err) {
            logger.error('[RiakCluster] error during shutdown:', err);
        }
        self._shutdown(callback);
    });
};

function makeStopNodeFunc(node) {
    var f = function (async_cb) {
        node.stop(function (err, rslt) {
            if (err) {
                logger.error('[RiakCluster] error stopping node (%s:%d):',
                    node.remoteAddress, node.remotePort, err);
            }
            if (async_cb) {
                async_cb(err, rslt);
            }
        });
    };
    return f;
}

RiakCluster.prototype._startNode = function (node, callback) {
    node.on(EVT_SC, this._onNodeStateChange.bind(this));
    node.on(EVT_RC, this._onRetryCommand.bind(this));
    node.start(function (err, rslt) {
        if (err) {
            logger.error('[RiakCluster] error starting node (%s:%d):',
                node.remoteAddress, node.remotePort, err);
        }
        if (callback) {
            callback(err, rslt);
        }
    });
};

RiakCluster.prototype._shutdown = function (callback) {
    logger.debug('[RiakCluster] checking to see if nodes are shut down.');

    var allStopped = true;
    for (var i = 0; i < this.nodes.length; i++) {
        var node = this.nodes[i];
        if (node.state !== RiakNode.State.SHUTDOWN) {
            allStopped = false;
            var stopNode = makeStopNodeFunc(node);
            stopNode();
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
        this.removeAllListeners();
        if (callback) {
            callback(null, this.state);
        }
    } else {
        logger.debug('[RiakCluster] nodes still running.');
        setTimeout(this._shutdown.bind(this, callback), 125);
    }
};

/**
 * Execute a command on this RiakCluster.
 *
 * Selects a RiakNode from the cluster and executes the command on it.
 * @method execute
 * @param {Object} riakCommand - the command to execute.
 * @param {RiakNode} [previous] the previous node this command was attempted on
 */
RiakCluster.prototype.execute = function(command, previous) {
    // If there's no previous node, set the remaining retries
    if (arguments.length === 1) {
        command.remainingTries = this.executionAttempts;
    }

    logger.debug('[RiakCluster] execute command: %s remaining tries: %d.',
        command.name, command.remainingTries);

    var executing = false;
    if (this._commandQueue.length === 0) {
        executing = this.nodeManager.executeOnNode(this.nodes, command, previous);
    }

    /*
     * NB: executing may be false if one or more nodes are NOT in the RUNNING
     * state, or if a node's connections are all in use.
     */
    if (!executing) {
        /*
         * NB: if commands ARE being queued, this will add the non-executed command
         * to the queue and schedule execution at a later time without decrementing
         * the re-try count.
         */
        if (this.queueCommands) {
            if (this.queueMaxDepth && (this._commandQueue.length >= this.queueMaxDepth)) {
                command.onError("No RiakNodes available and command queue at maxDepth");
            } else {
                this._commandQueue.push(command);
                if (this.state === State.RUNNING) {
                    this.state = State.QUEUEING;
                    // TODO: should this timeout be average command execution time? Used for rate-limiting
                    setTimeout(this._submitFromQueue.bind(this), this.queueSubmitInterval);
                    logger.info('[RiakCluster] queueing commands.');
                    this.emit(EVT_SC, this.state);
                }
            }
            return;
        }

        /*
         * NB: if commands are not being queued, but re-tries are available,
         * re-try the command to try and run it on a different node
         */
        if (command.remainingTries > 0) {
            command.remainingTries--;
            this._onRetryCommand(command, previous);
            return;
        }

        command.onError('No RiakNodes available to execute command.');
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
                // TODO: this does not take retries into account, so a command that continually
                // errors could back up the queue indefinitely
                this._commandQueue.unshift(command);
                // TODO: should this timeout be based on an average command execution time?
                setTimeout(this._submitFromQueue.bind(this), this.queueSubmitInterval);
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
 * Add a RiakNode to this cluster.
 * @method addNode
 * @param {RiakNode} node the (unstarted) RiakNode to add.
 */
RiakCluster.prototype.addNode = function(node) {
    var self = this;
    this._startNode(node, function (err, rslt) {
        self.nodes.push(node);
    });
};

/**
* Get a RiakNode index from this cluster.
* @method getNodeIndex
* @param {RiakNode|String} node - the node for getting the index. May be supplied as a RiakNode instance or IP|hostname[:port]
* @return {RiakNode} - The node index in this cluster. Returns -1 if the node does not exist.
*/
RiakCluster.prototype.getNodeIndex = function(node) {
    if (node instanceof RiakNode) {
        return this.nodes.indexOf(node);
    }

    // Hopefully it's a string "addr[:port]"
    var split = node.split(':');
    var addr = split[0];
    var port = (split[1])? Number(split[1]) : undefined;
    for (var i = 0; i < this.nodes.length; i++) {
        var n = this.nodes[i];
        if (n.remoteAddress === addr) {
            if (port === undefined || n.remotePort === port) {
                return i;
            }
        }
    }
    return -1;
};

/**
 * Remove a RiakNode from this cluster.
 * The node being removed will also be stopped.
 * @method removeNode
 * @param {RiakNode|String} node - the node to remove. May be supplied as a RiakNode instance or IP|hostname[:port]
 * @return {Boolean} - true if the node was removed.
 */
RiakCluster.prototype.removeNode = function(node) {
    var index = this.getNodeIndex(node);
    var n = this.nodes[index];
    if (n) {
        this.nodes.splice(index, 1);
        n.stop();
        return true;
    }
    return false;
};

RiakCluster.prototype._onNodeStateChange = function(node, state) {
    this.emit(EVT_NSC, node, state);
};

RiakCluster.prototype._onRetryCommand = function(command, lastNode) {
    var i = this.executionAttempts - command.remainingTries;
    // NB: only "immediately" re-try if there's another node on which to re-try
    if ((this.nodes.length > 1) && (i === 0 || i === 1)) {
        logger.debug('[RiakCluster] scheduling immediate re-try for:', command.name);
        setImmediate(this.execute.bind(this, command, lastNode));
    } else {
        var delay_ms = 100 * i;
        logger.debug('[RiakCluster] scheduling re-try with %d ms delay for:', delay_ms, command.name);
        setTimeout(this.execute.bind(this, command, lastNode), delay_ms);
    }
};

/**
 * The state of this cluster.
 *
 * If listening for stateChange events, a numeric value will be sent that
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
var State = Object.freeze({
    CREATED : 0,
    RUNNING : 1,
    QUEUEING: 2,
    SHUTTING_DOWN : 3,
    SHUTDOWN : 4
});

var stateNames = Object.freeze({
    0 : 'CREATED',
    1 : 'RUNNING',
    2 : 'QUEUEING',
    3 : 'SHUTTING_DOWN',
    4 : 'SHUTDOWN'
});

var defaultRiakNode = new RiakNode();
var defaultExecutionAttempts = 4;

function createDefaultNodeManager() {
    return new RoundRobinNodeManager();
}

var schema = Joi.object().keys({
    nodes: Joi.array().min(1).default([defaultRiakNode]),
    executionAttempts: Joi.number().min(1).default(defaultExecutionAttempts),
    nodeManager: Joi.object()
        .default(createDefaultNodeManager, 'default is a new instance of RoundRobinNodeManager'),
    queueCommands: Joi.boolean().default(false),
    queueMaxDepth: Joi.number().default(0),
    queueSubmitInterval: Joi.number().default(500)
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
     * In the case of a command failing for any reason, it will be retried
     * on a different node.
     * @method withExecutionAttempts
     * @param {Number} numAttempts - the number of times to attempt a command (__default:__ 3)
     * @return {RiakCluster.Builder}
     */
    withExecutionAttempts : function(numAttempts) {
        this.executionAttempts = numAttempts;
        return this;
    },
    /**
     * Set the NodeManager for this cluster.
     *
     * If not provided the {{#crossLink "RoundRobinNodeManager"}}{{/crossLink}} is
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
     * @param {Number} [submitInterval=500] The duration in milliseconds between queue submission attempts. Default is 500.
     * @chainable
     */
    withQueueCommands : function(maxDepth, submitInterval) {
        this.queueCommands = true;
        this.queueMaxDepth = maxDepth;
        this.queueSubmitInterval = submitInterval;
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
