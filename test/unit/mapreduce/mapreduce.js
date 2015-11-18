'use strict';

var MapReduce = require('../../../lib/commands/mapreduce/mapreduce');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');
var RpbMapRedResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbMapRedResp');

var assert = require('assert');

describe('MapReduce', function() {
    describe('Build', function() {
        it('should build a RpbMapRedReq correctly', function(done) {

            var query = '{"inputs":"goog","query":[{"map":{"language":"javascript",' +
                        '"source":"function(value, keyData, arg) { ' +
                        'var data = Riak.mapValuesJson(value)[0]; if(data.High ' +
                        '&& parseFloat(data.High) > 600.00) return [value.key];' +
                        'else return [];}","keep":true}}]}';

            var mr = new MapReduce(query, function(){});

            var protobuf = mr.constructPbRequest();

            assert.equal(protobuf.getRequest().toString('utf8'), query);
            assert.equal(protobuf.getContentType().toString('utf8'), 'application/json');
            done();

        });

        it('should take a RpbMapRedResp and call the users callback with the response', function(done) {
            
            var resp = new RpbMapRedResp();
            resp.phase = 1;
            // Riak returns JSON 
            resp.response = new Buffer('[{ "the": 8}]');
            resp.done = true;
            
            var callback = function(err, resp) {
                assert(resp.phase_1);
                // returned JSON is parsed and returned as JS
                assert.equal(resp.phase_1.constructor, Array);
                assert.equal(resp.phase_1.length, 1);
                assert.equal(resp.phase_1[0].the, 8);
                assert.equal(resp.done, true);
                done();
            };
            
            var mr = new MapReduce('some query', callback, false);
            mr.onSuccess(resp);
            
            
        });
        
        it('should take a RpbMapRedResp and stream the response', function(done) {
            
            var resp = new RpbMapRedResp();
            resp.phase = 1;
            // Riak returns JSON 
            resp.response = new Buffer('[{ "the": 8}]');
            resp.done = true;
            
            var callback = function(err, resp) {
                assert.equal(resp.phase, 1);
                // returned JSON is parsed and returned as JS
                assert.equal(resp.response.constructor, Array);
                assert.equal(resp.response.length, 1);
                assert.equal(resp.response[0].the, 8);
                assert.equal(resp.done, true);
                done();
            };
            
            var mr = new MapReduce('some query', callback);
            mr.onSuccess(resp);
            
            
        });
        
        it ('should take a RpbErrorResp and call the users callback with the error message', function(done) {
           var rpbErrorResp = new RpbErrorResp();
           rpbErrorResp.setErrmsg(new Buffer('this is an error'));
           
           var callback = function(err, response) {
               if (err) {
                   assert.equal(err,'this is an error');
                   done();
               }
           };
           
           var mr = new MapReduce('some query', callback);
           mr.onRiakError(rpbErrorResp);
       });
        
    });
});
