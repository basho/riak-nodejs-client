/**
 *
 * Copyright 2014-present Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

var logger = require('winston');

var RiakNode = require('./riaknode');

/**
 * @module Core
 */

/**
 * Abstract class that defines a NodeManager
 * 
 * Every time a command is executed on the {{#crossLink "RiakCluster"}}{{/crossLink}} 
 * a {{#crossLink "RiakNode"}}{{/crossLink}} is selected. The default procedure for 
 * doing so is a simple round-robin provided via {{#crossLink "DefaultNodeManager"}}{{/crossLink}}.
 * 
 * If you wish to alter this behavior you should extend this class and implement 
 * your own executeOnNode(). This function should pick a node from the list and 
 * execute the command via {{#crossLink "RiakNode/execute:method"}}{{/crossLink}}. 
 * 
 * @class NodeManager
 * @param {String} name The name of the node manager
 * @constructor
 */
function NodeManager(name) {
    this._name = name;
}

/**
 * Receives the array or RiakNode objects from the RiakCluster, chooses one, and executes the command on it.
 * @param {RiakNode[]} nodes The array of nodes contained in the RiakCluster
 * @param {Object} command The command to execute on a node.
 * @param {RiakNode} [previous] if a command is being retried due to a failure, this will be the previous node on which it was attempted.
 * @returns {Boolean} True if a node was chosen and accepted the command, false otherwise.
 */
NodeManager.prototype.executeOnNode = function(nodes, command, previous) {
    throw 'Not supported yet!';
};

/**
 * @param {RiakNode} node The node on which to try to execute the command
 * @param {Object} command The command to execute on a node.
 * @returns {Boolean} True if a node was chosen and accepted the command, false otherwise.
 */
NodeManager.prototype.tryExecute = function(node, command) {
    var executing = false;
    if (node.state === RiakNode.State.RUNNING) {
        logger.debug("[%s] executing command '%s' on node (%s:%d)",
            this._name, command.name, node.remoteAddress, node.remotePort);
        if (node.execute(command)) {
            executing = true;
        } else {
            logger.debug("[%s] command '%s' did NOT execute",
                this._name, command.name);
        }
    }
    return executing;
};

module.exports = NodeManager;
