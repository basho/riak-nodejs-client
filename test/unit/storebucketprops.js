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

var StoreBucketProps = require('../../lib/commands/kv/storebucketprops');
var RpbErrorResp = require('../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');
var RpbCommitHook = require('../../lib/protobuf/riakprotobuf').getProtoFor('RpbCommitHook');
var RpbModFun = require('../../lib/protobuf/riakprotobuf').getProtoFor('RpbModFun');
var assert = require('assert');

describe('StoreBucketProps', function() {
    describe('Build', function() {
        it('should build a RpbSetBucketProps correctly', function(done) {
            
            var hook = { mod: 'module_name', fun: 'function_name' };
            
            var storeProps = new StoreBucketProps.Builder()
                .withBucketType('bucket_type')
                .withBucket('bucket_name')
                .withNVal(3)
                .withAllowMult(true)
                .withLastWriteWins(true)
                .withOldVClock(86400)
                .withYoungVClock(20)
                .withBigVClock(50)
                .withSmallVClock(51)
                .withR(1)
                .withPr(2)
                .withW(3)
                .withPw(4)
                .withDw(5)
                .withRw(6)
                .withBasicQuorum(false)
                .withNotFoundOk(true)
                .withSearch(true)
                .withBackend('backend')
                .withSearchIndex('indexName')
                .addPrecommitHook(hook)
                .addPostcommitHook(hook)
                .withChashkeyFunction(hook)
                .withCallback(function(){})
                .build();
        
            var protobuf = storeProps.constructPbRequest();
            var props = protobuf.getProps();
            
            assert.equal(protobuf.bucket.toString('utf8'), 'bucket_name');
            assert.equal(protobuf.type.toString('utf8'), 'bucket_type');
            assert.equal(props.getNVal(), 3);
            assert.equal(props.getAllowMult(), true);
            assert.equal(props.getLastWriteWins(), true);
            assert.equal(props.getOldVclock(), 86400);
            assert.equal(props.getYoungVclock(), 20);
            assert.equal(props.getBigVclock(), 50);
            assert.equal(props.getSmallVclock(), 51);
            assert.equal(props.r, 1);
            assert.equal(props.pr, 2);
            assert.equal(props.w, 3);
            assert.equal(props.pw, 4);
            assert.equal(props.dw, 5);
            assert.equal(props.rw, 6);
            assert.equal(props.getBasicQuorum(), false);
            assert.equal(props.getNotfoundOk(), true);
            assert.equal(props.getSearch(), true);
            assert.equal(props.getBackend().toString('utf8'), 'backend');
            assert.equal(props.getSearchIndex().toString('utf8'), 'indexName');
            
            assert.equal(props.precommit.length, 1);
            assert.equal(props.precommit[0].modfun.module.toString('utf8'), 'module_name');
            assert.equal(props.precommit[0].modfun.function.toString('utf8'), 'function_name');
            
            assert.equal(props.postcommit.length, 1);
            assert.equal(props.postcommit[0].modfun.module.toString('utf8'), 'module_name');
            assert.equal(props.postcommit[0].modfun.function.toString('utf8'), 'function_name');
            
            assert.equal(props.getChashKeyfun().module.toString('utf8'), 'module_name');
            assert.equal(props.getChashKeyfun().function.toString('utf8'), 'function_name');
            done();
            
                
        });
        
        it('should take a RpbGetBucketResp and call the users callback with the response', function(done) {
           
            // RpbSetBucketResp has no body. Riak just sends back the code so we supply null
            // to the command on success and a simple boolean true is sent to the user callback
            
            var callback = function(err, response) {
                assert.equal(response, true);
                done();
            };
            
            var storeProps = new StoreBucketProps.Builder()
                .withBucketType('bucket_type')
                .withBucket('bucket_name')
                .withCallback(callback)
                .build();
        
            storeProps.onSuccess(null);
            
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
           
           var storeProps = new StoreBucketProps.Builder()
                .withBucketType('bucket_type')
                .withBucket('bucket_name')
                .withCallback(callback)
                .build();
        
            storeProps.onRiakError(rpbErrorResp);
           
        });
    });
});
        