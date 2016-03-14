var inherits = require('util').inherits;
var logger = require('winston');

var NodeManager = require('./nodemanager');
var RiakNode = require('./riaknode');

/**
 * @module Core
 */

/**
 * A NodeManager that can be used by RiakCluster.
 * 
 * This NodeManager does a round-robin selection of RiakNodes.
 * 
 * @class RoundRobinNodeManager
 * @constructor
 * @extends NodeManager 
 */
function RoundRobinNodeManager()  {
    NodeManager.call(this, 'RoundRobinNodeManager');
    this._nodeIndex = 0;
}

inherits(RoundRobinNodeManager, NodeManager);

RoundRobinNodeManager.prototype.executeOnNode = function(nodes, command, previous) {
    if (nodes.length === 0) {
        logger.error('[RoundRobinNodeManager] zero nodes for execution of command %s', command.name);
        return false;
    }

    var executing = false;
    var first = true;

    var startingIndex = this._nodeIndex;
    if (startingIndex >= nodes.length) {
        startingIndex = 0;
    }

    for (;;) {
        // Check index before accessing {nodes} because elements can be removed from {nodes}.
        if (this._nodeIndex >= nodes.length) {
            this._nodeIndex = 0;
        }

        if (!first && (this._nodeIndex === startingIndex || nodes.length === 1)) {
            break;
        }

        first = false;

        var node = nodes[this._nodeIndex];
        this._nodeIndex++;

        // don't try the same node twice in a row if we have multiple nodes
        if (nodes.length > 1 && previous && previous === node) {
            continue;
        }

        if (node.state !== RiakNode.State.RUNNING) {
            continue;
        }

        executing = this.tryExecute(node, command);
        if (executing) {
            break;
        }
    } 
    
    return executing;
    
};

module.exports = RoundRobinNodeManager;
