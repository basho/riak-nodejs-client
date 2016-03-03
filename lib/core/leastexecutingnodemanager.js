var inherits = require('util').inherits;
var logger = require('winston');

var NodeManager = require('./nodemanager');

/**
 * @module Core
 */

/**
 * A NodeManager that can be used by RiakCluster.
 * 
 * This NodeManager does a least-commands-executing selection of RiakNodes.
 * 
 * @class LeastExecutingNodeManager
 * @param {Boolean} shuffle Shuffle nodes that have same execution count.
 * @constructor
 * @extends NodeManager 
 */
function LeastExecutingNodeManager(shuffle)  {
    NodeManager.call(this, 'LeastExecutingNodeManager');
    this._shuffle = shuffle;
}

inherits(LeastExecutingNodeManager, NodeManager);

LeastExecutingNodeManager.prototype.executeOnNode = function(nodes, command, previous) {
    if (nodes.length === 0) {
        logger.error("[LeastExecutingNodeManager] zero nodes for execution of command %s", command.name);
        return false;
    }

    var n = [];
    Array.prototype.push.apply(n, nodes);
    n.sort(function (a, b) {
        return a.executeCount - b.executeCount;
    });

    var i = 0;
    if (this._shuffle) {
        var j = 0;
        for (i = 0; i < n.length - 1; i++) {
            j = i + 1;
            if (n[j].executeCount > n[i].executeCount) {
                break;
            }
        }
        if (j > 1) {
            var s = shuffleArray(n.slice(0, j));
            n = Array.prototype.concat(s, n.slice(j));
        }
    }

    var executing = false;
    for (i = 0; i < n.length; i++) {
        executing = this.tryExecute(n[i], command);
        if (executing) {
            break;
        }
    }
    return executing;
};

/**
 * Randomize array element order in-place.
 * Using Durstenfeld shuffle algorithm.
 * http://stackoverflow.com/a/12646864
 */
function shuffleArray(array) {
    for (var i = array.length - 1; i > 0; i--) {
        var j = Math.floor(Math.random() * (i + 1));
        var temp = array[i];
        array[i] = array[j];
        array[j] = temp;
    }
    return array;
}

module.exports = LeastExecutingNodeManager;
