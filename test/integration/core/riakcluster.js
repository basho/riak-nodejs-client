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

var RiakCluster = require('../../../lib/core/riakcluster');
var RiakNode = require('../../../lib/core/riaknode');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');
var FetchValue = require('../../../lib/commands/kv/fetchvalue');
var StoreValue = require('../../../lib/commands/kv/storevalue');
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
    
    describe('Command queueing', function() {

        it('Should queue commands and retry from the queue, respecting queueSubmitInterval', function(done) {

           var server = Net.createServer(function(socket) {
              
               socket.on('data', function(data) {
                  
                   // it got here
                   var header = new Buffer(5);
                    header.writeUInt8(12, 4);
                    header.writeInt32BE(1, 0);
                    socket.write(header);
                   
               });
               
           });
           
           var storeCheck = new StoreValue({bucket: 'b', value: 'v'}, function(){});
           var node = new RiakNode.Builder()
                   .withRemotePort(1337)
                   .withHealthCheck(storeCheck)
                   .build();         
           
           var cluster = new RiakCluster.Builder()
                   .withRiakNodes([node])
                   .withQueueCommands(undefined, 600)
                   .build();

           var queueStart;

           var stateMe = function(state) {
                assert.equal(typeof(state), 'number', 'stateType');
                if (state === RiakCluster.State.QUEUEING) {
                    queueStart = Date.now();
                    server.listen(1337, '127.0.0.1');
                }
            };
            
            cluster.on('stateChange', stateMe);
            cluster.start();
            
            var callMe = function(err, resp) {
                assert(!err, err);
                assert(Date.now() - queueStart >= 600, 'queueSubmitInterval respected');
                cluster.stop();
                server.close();
                done();
            };
            
            var store = new StoreValue({bucket: 'b', value: 'v'}, callMe);
            
            cluster.execute(store);
            
        });
        
        it('should not queue by default', function(done) {
            
            var node = new RiakNode.Builder()
                   .withRemotePort(1337)
                   .build();         

            var cluster = new RiakCluster.Builder()
                   .withRiakNodes([node])
                   .build();

            cluster.start();

            var callMe = function(err, resp) {
                assert(err);
                cluster.stop();
                done();
            };

            var store = new StoreValue({bucket: 'b', value: 'v'}, callMe);

            cluster.execute(store);
            
        });
        
        it ('should not queue if maxDepth is reached', function(done) {
           
            var node = new RiakNode.Builder()
                   .withRemotePort(1337)
                   .build();         

            var cluster = new RiakCluster.Builder()
                   .withRiakNodes([node])
                   .withQueueCommands(1)
                   .build();

            cluster.start();

            var callMe = function(err, resp) {
                assert(err);
                cluster.stop();
                done();
            };

            var store = new StoreValue({bucket: 'b', value: 'v'}, callMe);

            cluster.execute(store);
            cluster.execute(store);
            
        });
        
    });
    
});
