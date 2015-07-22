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

var NodeManager = require('./nodemanager');
var RiakNode = require('./riaknode');
var inherits = require('util').inherits;

/**
 * @module Core
 */

/**
 * The default NodeManager used by RiakCluster.
 * 
 * This NodeManager does a round-robin selection of RiakNodes.
 * 
 * @class DefaultNodeManager
 * @constructor
 * @extends NodeManager 
 */
function DefaultNodeManager()  {
    NodeManager.call(this);
    this._nodeIndex = 0;
}

inherits(DefaultNodeManager, NodeManager);

DefaultNodeManager.prototype.executeOnNode = function(nodes, command, previous) {

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
        
        if (node.state === RiakNode.State.RUNNING) {
            if (node.execute(command)) {
                executing = true;
                break;
            }
        }
        
    } while (this._nodeIndex !== startingIndex);
    
    return executing;
    
};

module.exports = DefaultNodeManager;
