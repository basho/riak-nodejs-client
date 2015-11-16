var assert = require('assert');
var logger = require('winston');
var net = require('net');

var Test = require('../testparams');
var RiakNode = require('../../../lib/core/riaknode');
var Ping = require('../../../lib/commands/ping');
var FetchValue = require('../../../lib/commands/kv/fetchvalue');
var StoreValue = require('../../../lib/commands/kv/storevalue');

describe('RiakNode - Integration', function() {
   
    describe('Command execution', function() {
        this.timeout(5000);

        it('should increment execution count', function(done) {
          
            var port = Test.getPort();
            var header = new Buffer(5);
            header.writeUInt8(2, 4);
            header.writeInt32BE(1, 0);

            var server = net.createServer(function(socket) {
                socket.on('data', function(data) {
                    socket.write(header);
                });
            });

            var lcb = function () {
                var scb = function () {
                    var pingCount = 0;
                    var cb = function(err, resp) {
                        pingCount++;
                        assert(!err, err);
                        assert(resp, 'ping should return true!');
                        if (pingCount == 4) {
                            node.stop();
                            server.close(function () {
                                done();
                            });
                        }
                    };
                    var cmd = new Ping(cb);

                    assert.equal(node.executeCount, 0);

                    assert(node.execute(cmd));
                    assert.equal(node.executeCount, 1);

                    assert(node.execute(cmd));
                    assert.equal(node.executeCount, 2);

                    assert(node.execute(cmd));
                    assert.equal(node.executeCount, 3);

                    assert(node.execute(cmd));
                    assert.equal(node.executeCount, 4);
                };

                var node = new RiakNode.Builder()
                    .withRemotePort(port)
                    .withMinConnections(8)
                    .build();
                node.start(scb);
            };
            
            server.listen({ host: '127.0.0.1', port: port }, lcb);
        });
    });

    describe('Health checking', function() {
        this.timeout(5000);
       
        it('should recover using the default Ping check', function(done) {
          
            var port = Test.getPort();
            var connects = 0;
            var server = net.createServer(function(socket) {
              
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
            
            server.listen(port, '127.0.0.1', function () {
                var errTimeout = setTimeout(function () {
                    assert(false, 'Event never fired');
                    done();
                }, 3000); 
                
                var node = new RiakNode.Builder()
                        .withRemotePort(port)
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
                        server.close(function () {
                            done();
                        });
                    }
                };
                
                node.start(function (err, rslt) {
                    assert(!err, err);

                    node.on('stateChange', verifyCb);
                    
                    var fetchCb = function(err, resp) {
                        assert(err);
                    };
                    
                    var fetch = new FetchValue({bucket: 'b', key: 'k'}, fetchCb);
                    node.execute(fetch);
                });
            });
        });
        
        it('should recover using StoreValue as a check', function(done) {
          
            var port = Test.getPort();
            var connects = 0;
            var server = net.createServer(function(socket) {
              
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
            
            server.listen(port, '127.0.0.1', function () {
                var errTimeout = setTimeout(function () {
                    assert(false, 'Event never fired');
                    done();
                }, 3000); 
                
                var storeCheck = new StoreValue({bucket: 'b', value: 'v'}, function(){});
                
                var node = new RiakNode.Builder()
                        .withRemotePort(port)
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
                        server.close(function () {
                            done();
                        });
                    }
                };
                
                node.start(function (err, rslt) {
                    assert(!err, err);

                    node.on('stateChange', verifyCb);
                    
                    var fetchCb = function(err, resp) {
                        assert(err);
                    };
                    
                    var fetch = new FetchValue({bucket: 'b', key: 'k'}, fetchCb);
                    node.execute(fetch);
                });
            });
        });
    });
});
