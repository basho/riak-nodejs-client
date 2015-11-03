'use strict';

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

var rpb = require('../../../lib/protobuf/riakprotobuf');
var RiakConnection = require('../../../lib/core/riakconnection');
var RpbErrorResp = rpb.getProtoFor('RpbErrorResp');
var responseCode = rpb.getCodeFor('RpbErrorResp');
var RpbGetResp = rpb.getProtoFor('RpbGetResp');
var RpbContent = rpb.getProtoFor('RpbContent');
var rpbGetRespCode = rpb.getCodeFor('RpbGetResp');

var assert = require('assert');
var logger = require('winston');

describe('RiakConnection', function() {
    describe('#_receiveData', function() {
        
        var conn = new RiakConnection({ remoteAddress : "127.0.0.1",
                                            remotePort : 8087,
                                            connectionTimeout : 30000
                                          });
        var resp = new RpbErrorResp();
        var msg = new Buffer('some error message');
        resp.setErrmsg(msg);
        resp.setErrcode(5);                                  
        var header = new Buffer(5);
        header.writeUInt8(responseCode, 4);
        var encoded = resp.encode().toBuffer();
        header.writeInt32BE(encoded.length + 1, 0);
        
        it('should emit responseReceived', function(done) {
            
            var errTimeout = setTimeout(function () {
                assert(false, 'Event never fired');
                done();
            }, 1000); 
            
            conn.on('responseReceived', function(conn, command, code, decoded) {
                clearTimeout(errTimeout);
                assert(code === responseCode);
                assert(decoded.getErrmsg().toBuffer().toString() === msg.toString());
                assert(true);
                conn.removeAllListeners();
                done();
            });
            
            conn._receiveData(header);
            conn._receiveData(encoded);
            
        });
        
        it('should emit responseReceived twice', function(done) {
            
            var errTimeout = setTimeout(function () {
                assert(false, 'Event never fired twice'); 
                done();
            }, 1000); 
            
            var count = 0;
            conn.on('responseReceived', function(conn, command, code, decoded) {
                count++;
                assert(code === responseCode);
                assert(decoded.getErrmsg().toBuffer().toString() === msg.toString());
                if (count === 2) {
                    clearTimeout(errTimeout);
                    assert(true);
                    conn.removeAllListeners();
                    done();
                }
            });
            
            for (var i = 0; i < 2; i++) {
                conn._receiveData(header);
                conn._receiveData(encoded);
            }
        });
        
        it('should buffer a partial message', function() {
            
            conn._receiveData(header);

            assert(conn._buffer.flip().remaining() === 5);
            conn._buffer.clear();
        
        });
        
        it('should emit then buffer partial second message', function(done) {
            
            var errTimeout = setTimeout(function () {
                assert(false, 'Event never fired'); 
                done();
            }, 1000); 
            
            conn.on('responseReceived', function(conn, command, code, decoded) {
                clearTimeout(errTimeout);
                assert(code === responseCode);
                assert(decoded.getErrmsg().toBuffer().toString() === msg.toString());
                assert(conn._buffer.flip().remaining() === 5);
                assert(conn._buffer.offset === 0);
                conn.removeAllListeners();
                conn._buffer.clear();
                done();
            });
            
            var combined = Buffer.concat([header, encoded, header]);
            
            conn._receiveData(combined);
            
        });

        it('should not clobber data when decoding buffer', function(done) {

            var vclocks = ['vclock1234', 'vclock5678'];

            var errTimeout = setTimeout(function () {
                assert(false, 'Event never fired');
                done();
            }, 1000); 
            
            var decodedMessages = [];

            conn.on('responseReceived', function(conn, command, code, decoded) {
                clearTimeout(errTimeout);
                assert(code === rpbGetRespCode);
                decodedMessages.push(decoded);
                if (decodedMessages.length === vclocks.length) {
                    var i = 0;
                    for (i = 0; i < vclocks.length; ++i) {
                        var vclock = vclocks[i];
                        var decodedVclock = decodedMessages[i].getVclock().toString('utf8');
                        logger.debug("[test/unit/riakconnection] i '%d' vclock '%s' decodedVclock '%s'",
                            i, vclock, decodedVclock);
                        assert.equal(decodedVclock, vclock);
                    }
                    conn.removeAllListeners();
                    done();
                }
            });
            
            vclocks.forEach(function (vclock) {
                var rpbContent = new RpbContent();
                rpbContent.setValue(new Buffer('this is a value'));
                rpbContent.setContentType(new Buffer('application/json'));
                
                var rpbGetResp = new RpbGetResp();
                rpbGetResp.setContent(rpbContent);
                rpbGetResp.setVclock(new Buffer(vclock));

                var header = new Buffer(5);
                header.writeUInt8(rpbGetRespCode, 4);

                var encoded = rpbGetResp.encode().toBuffer();
                var encodedLength = encoded.length + 1;
                header.writeInt32BE(encodedLength, 0);

                conn._receiveData(header);
                conn._receiveData(encoded);
            });
        });
        
    });

});
