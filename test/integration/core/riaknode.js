'use strict';

var RiakNode = require('../../../lib/core/riaknode');
var assert = require('assert');
var Ping = require('../../../lib/commands/ping');
var FetchValue = require('../../../lib/commands/kv/fetchvalue');
var StoreValue = require('../../../lib/commands/kv/storevalue');

var Net = require('net');

describe('RiakNode - Integration', function() {
   
    describe('Health checking', function() {
       
        this.timeout(5000);
       
        it('should recover using the default Ping check', function(done) {
          
            var connects = 0;
            var server = Net.createServer(function(socket) {
              
                connects++;
                if (connects === 1) {
                    socket.destroy();
                } else {

                    socket.on('data' , function(data) {

                        // the ping got here
                        var header = new Buffer(5);
                        header.writeUInt8(2, 4);
                        header.writeInt32BE(1, 0);
                        socket.write(header);

                    });
                }
            });
            
            server.listen(1337, '127.0.0.1');
            
            var errTimeout = setTimeout(function () {
                assert(false, 'Event never fired');
                done();
            }, 3000); 
            
            var node = new RiakNode.Builder()
                    .withRemotePort(1337)
                    .withMinConnections(0)
                    .build();
            
            var heathChecking = false;
            var healthChecked = false;
            var verifyCb = function(node, state) {
              
                switch(state) {
                    case RiakNode.State.HEALTH_CHECKING:
                        heathChecking = true;
                        break;
                    case RiakNode.State.RUNNING:
                        healthChecked = true;
                        break;
                    default:
                        break;
                }
                
                if (heathChecking && healthChecked) {
                    clearTimeout(errTimeout);
                    node.removeAllListeners();
                    node.stop();
                    server.close();
                    done();
                }
                
            };
            
            node.start();
            node.on('stateChange', verifyCb);
            
            var fetchCb = function(err, resp) {
                assert(err);
            };
            
            var fetch = new FetchValue({bucket: 'b', key: 'k'}, fetchCb);
            node.execute(fetch);
            
        });
        
        it('should recover using StoreValue as a check', function(done) {
          
            var connects = 0;
            var server = Net.createServer(function(socket) {
              
                connects++;
                if (connects === 1) {
                    socket.destroy();
                    
                } else {

                    socket.on('data' , function(data) {

                        // the StoreValue got here
                        var header = new Buffer(5);
                        header.writeUInt8(12, 4);
                        header.writeInt32BE(1, 0);
                        socket.write(header);

                    });
                }
            });
            
            server.listen(1337, '127.0.0.1');
            
            var errTimeout = setTimeout(function () {
                assert(false, 'Event never fired');
                done();
            }, 3000); 
            
            
            var storeCheck = new StoreValue({bucket: 'b', value: 'v'}, function(){});
            
            var node = new RiakNode.Builder()
                    .withRemotePort(1337)
                    .withMinConnections(0)
                    .withHealthCheck(storeCheck)
                    .build();
            
            var heathChecking = false;
            var healthChecked = false;
            var verifyCb = function(node, state) {
              
                switch(state) {
                    case RiakNode.State.HEALTH_CHECKING:
                        heathChecking = true;
                        break;
                    case RiakNode.State.RUNNING:
                        healthChecked = true;
                        break;
                    default:
                        break;
                }
                
                if (heathChecking && healthChecked) {
                    clearTimeout(errTimeout);
                    node.removeAllListeners();
                    node.stop();
                    server.close();
                    done();
                }
                
            };
            
            node.start();
            node.on('stateChange', verifyCb);
            
            var fetchCb = function(err, resp) {
                assert(err);
            };
            
            var fetch = new FetchValue({bucket: 'b', key: 'k'}, fetchCb);
            node.execute(fetch);
            
        });
        
    });
    
});
