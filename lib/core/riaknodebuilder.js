/*
 * Copyright 2014 Basho Technologies, Inc.
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

var RiakNode = require('./riaknode');

function RiakNodeBuilder() {}
    
RiakNodeBuilder.prototype = {
  
    withRemoteAddress : function(address) {
        this.remoteAddress = address;
        return this;
    },
    
    withRemotePort : function(port) {
        this.remotePort = port;
        return this;
    },
    
    withMinConnections : function(minConnections) {
        this.minConnections = minConnections;
        return this;
    },
    
    withMaxConnections : function(maxConnections) {
        this.maxConnections = maxConnections;
        return this;
    },
    
    withIdleTimeout : function(idleTimeout) {
        this.idleTimeout = idleTimeout;
        return this;
    },
    
    withConnectionTimeout : function(connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    },
    
    build : function() {
        return new RiakNode(this);
    }
    
};

module.exports = RiakNodeBuilder;