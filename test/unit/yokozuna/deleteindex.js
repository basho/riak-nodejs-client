'use strict';

var DeleteIndex = require('../../../lib/commands/yokozuna/deleteindex');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');

var assert = require('assert');

describe('DeleteIndex', function() {
    describe('Build', function() {
        it('should build a RpbYokozunaIndexDeleteReq correctly', function(done) {
            
            var del = new DeleteIndex.Builder()
                    .withIndexName('indexName')
                    .withCallback(function(){})
                    .build();
            
            var protobuf = del.constructPbRequest();
            
            assert.equal(protobuf.getName().toString('utf8'), 'indexName');
            done();
            
        });
        
        it('should take a RpbDelResp and call the users callback with the response', function(done) {
           
            
            var callback = function(err, response) {
              
                assert.equal(response, true);
                done();
            };
            
            var del = new DeleteIndex.Builder()
                    .withIndexName('indexName')
                    .withCallback(callback)
                    .build();
            
            // a RpbDelResp is a null message (no body) from riak
            del.onSuccess(null);
            
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
           
           var del = new DeleteIndex.Builder()
                    .withIndexName('indexName')
                    .withCallback(callback)
                    .build();
            
            del.onRiakError(rpbErrorResp);
           
       });

        

    });
});
