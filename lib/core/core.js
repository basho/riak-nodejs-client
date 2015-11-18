'use strict';

/**
 * Provides the classes that make up the core of the client.
 * @module Core
 * @main Core
 */
function Core() { }

// Core exports
module.exports = Core;
module.exports.RiakNode = require('./riaknode');
module.exports.RiakCluster = require('./riakcluster');

var RoundRobinNodeManager = require('./roundrobinnodemanager');
module.exports.RoundRobinNodeManager = RoundRobinNodeManager;
module.exports.DefaultNodeManager = RoundRobinNodeManager;

module.exports.LeastExecutingNodeManager = require('./leastexecutingnodemanager');
