var inherits = require('util').inherits;
var logger = require('winston');

var NodeManager = require('./nodemanager');

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

    var executing = false;
    var startingIndex = this._nodeIndex;
    do {
        // Check index before accessing {nodes} because elements can be removed from {nodes}.
        if (this._nodeIndex >= nodes.length) {
            this._nodeIndex = 0;
        }
        var node = nodes[this._nodeIndex];
        this._nodeIndex++;

        // don't try the same node twice in a row if we have multiple nodes
        if (nodes.length > 1 && previous && previous === node) {
            continue;
        }
        
        executing = this.tryExecute(node, command);
        if (executing) {
            break;
        }
    } while (this._nodeIndex !== startingIndex);
    
    return executing;
    
};

module.exports = RoundRobinNodeManager;
