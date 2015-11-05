'use strict';

var FetchIndex = require('../../../lib/commands/yokozuna/fetchindex');
var RpbYokozunaIndexGetResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbYokozunaIndexGetResp');
var RpbYokozunaIndex = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbYokozunaIndex');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');

var assert = require('assert');

describe('FetchIndex', function() {
    describe('Build', function() {
        it('should build a RpbYokozunaIndexGetReq correctly', function(done) {
            
            var fetch = new FetchIndex.Builder()
                    .withIndexName('indexName')
                    .withCallback(function(){})
                    .build();
            
            var protobuf = fetch.constructPbRequest();
            
            assert.equal(protobuf.getName().toString('utf8'), 'indexName');
            done();
            
        });
        
        it('should take a RpbYokozunaIndexGetResp and call the users callback with the response', function(done) {
           
            var resp = new RpbYokozunaIndexGetResp();
            
            for (var i =0; i < 3; i++) {
                var index = new RpbYokozunaIndex();

                index.name = new Buffer('myIndex_' + i);
                index.schema = new Buffer('mySchema_' + i);
                index.n_val = i;

                resp.index.push(index);
            }
            
            var callback = function(err, response) {
              
                assert.equal(response.length, 3);
                assert.equal(response[1].indexName, 'myIndex_1');
                assert.equal(response[1].schemaName, 'mySchema_1');
                assert.equal(response[1].nVal, 1);
                done();
            };
            
            var fetch = new FetchIndex.Builder()
                    .withIndexName('indexName')
                    .withCallback(callback)
                    .build();
            
            fetch.onSuccess(resp);
            
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
           
           var fetch = new FetchIndex.Builder()
                    .withIndexName('indexName')
                    .withCallback(callback)
                    .build();
            
            fetch.onRiakError(rpbErrorResp);
           
       });
        
    });
});
