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

var RiakCluster = require('../../../lib/core/riakcluster');
var RiakNode = require('../../../lib/core/riaknode');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');
var FetchValue = require('../../../lib/commands/kv/fetchvalue');
var assert = require('assert');
var Net = require('net');

describe('RiakCluster - Integration', function() {
   
    describe('Node selection', function() {
       
        it('should try all three nodes with the default NodeManager', function(done) {
           
           
            var port = 1337;
            var servers = new Array(3);
            var nodes = new Array(3);
            var i;
            var tried = 0;
            
            var header = new Buffer(5);
            header.writeUInt8(0, 4);

            var err = new RpbErrorResp();
            err.setErrmsg(new Buffer('Some error'));
            err.setErrcode(0);
            var encoded = err.encode().toBuffer();
            header.writeInt32BE(encoded.length + 1, 0);
            
            var sockMe = function(socket) {
                socket.on('data' , function(data) {

                    // the fetch got here
                    socket.write(header);
                    socket.write(encoded);


                    tried++;
                    if (tried === 3) {
                        clearTimeout(errTimeout);
                        cluster.stop();
                        for (var j = 0; j < 3; j++) {
                            servers[j].close();
                        }
                        done();
                    }
                });
            };
            
            for (i = 0; i < 3; i++, port++) {
                servers[i] = Net.createServer(sockMe);
            
                servers[i].listen(port, '127.0.0.1');
                
                nodes[i] = new RiakNode.Builder()
                    .withRemotePort(port)
                    .withMinConnections(0)
                    .build();
                
            }
            
            var errTimeout = setTimeout(function () {
                assert(false, 'All nodes weren\'t tried');
                done();
            }, 3000); 
            
            var cluster = new RiakCluster({nodes: nodes});
            cluster.start();
            
            var fetch = new FetchValue({bucket: 'b', key: 'k'}, function(){});
            cluster.execute(fetch);
            
            
        });
        
    });
    
});