'use strict';

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
 * @constructor
 */
function NodeManager() {}


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

module.exports = NodeManager;
