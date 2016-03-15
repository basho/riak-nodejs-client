'use strict';

var assert = require('assert');
var logger = require('winston');

var rpb = require('../../../lib/protobuf/riakprotobuf');
var RiakConnection = require('../../../lib/core/riakconnection');
var RpbErrorResp = rpb.getProtoFor('RpbErrorResp');
var responseCode = rpb.getCodeFor('RpbErrorResp');
var RpbGetResp = rpb.getProtoFor('RpbGetResp');
var RpbContent = rpb.getProtoFor('RpbContent');
var rpbGetRespCode = rpb.getCodeFor('RpbGetResp');

function getConn() {
    return new RiakConnection({
        remoteAddress : "127.0.0.1",
        remotePort : 8087,
        connectionTimeout : 30000
    });
}

describe('RiakConnection', function() {
    describe('#_receiveData', function() {
        var resp = new RpbErrorResp();
        var msg = new Buffer('some error message');
        resp.setErrmsg(msg);
        resp.setErrcode(5);                                  
        var header = new Buffer(5);
        header.writeUInt8(responseCode, 4);
        var encoded = resp.encode().toBuffer();
        header.writeInt32BE(encoded.length + 1, 0);
        
        it('should emit responseReceived', function(done) {
            var conn = getConn();
            conn.on('responseReceived', function(conn, command, code, decoded) {
                assert.strictEqual(code, responseCode);
                assert.strictEqual(decoded.getErrmsg().toBuffer().toString(), msg.toString());
                conn.close();
                done();
            });
            conn._receiveData(header);
            conn._receiveData(encoded);
        });
        
        it('should emit responseReceived twice', function(done) {
            var conn = getConn();
            var count = 0;
            conn.on('responseReceived', function(conn, command, code, decoded) {
                count++;
                assert.strictEqual(code, responseCode);
                assert.strictEqual(decoded.getErrmsg().toBuffer().toString(), msg.toString());
                if (count === 2) {
                    conn.close();
                    done();
                }
            });
            for (var i = 0; i < 2; i++) {
                conn._receiveData(header);
                conn._receiveData(encoded);
            }
        });
        
        it('should emit then buffer partial second message', function(done) {
            var conn = getConn();
            conn.on('responseReceived', function(conn, command, code, decoded) {
                assert.strictEqual(code, responseCode);
                assert.strictEqual(decoded.getErrmsg().toBuffer().toString(), msg.toString());
                conn.close();
                done();
            });
            var combined = Buffer.concat([header, encoded, header]);
            conn._receiveData(combined);
        });

        it('should not clobber data when decoding buffer', function(done) {
            var conn = getConn();
            var vclocks = ['vclock1234', 'vclock5678'];
            var decodedMessages = [];
            conn.on('responseReceived', function(conn, command, code, decoded) {
                assert.strictEqual(code, rpbGetRespCode);
                decodedMessages.push(decoded);
                if (decodedMessages.length === vclocks.length) {
                    var i = 0;
                    for (i = 0; i < vclocks.length; ++i) {
                        var vclock = vclocks[i];
                        var decodedVclock = decodedMessages[i].getVclock().toString('utf8');
                        logger.debug("[test/unit/riakconnection] i '%d' vclock '%s' decodedVclock '%s'",
                            i, vclock, decodedVclock);
                        assert.strictEqual(decodedVclock, vclock);
                    }
                    conn.close();
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
