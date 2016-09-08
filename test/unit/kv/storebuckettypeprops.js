'use strict';

var rpb = require('../../../lib/protobuf/riakprotobuf');
var StoreBucketTypeProps = require('../../../lib/commands/kv/storebuckettypeprops');
var RpbErrorResp = rpb.getProtoFor('RpbErrorResp');
var RpbCommitHook = rpb.getProtoFor('RpbCommitHook');
var RpbModFun = rpb.getProtoFor('RpbModFun');
var assert = require('assert');

describe('StoreBucketTypeProps', function() {
    describe('Build', function() {
        it('should build a RpbSetBucketTypeProps correctly', function(done) {
            var hook = { mod: 'module_name', fun: 'function_name' };
            var storeProps = new StoreBucketTypeProps.Builder()
                .withBucketType('bucket_type')
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
                .withHllPrecision(14)
                .withCallback(function(){})
                .build();

            var protobuf = storeProps.constructPbRequest();
            var props = protobuf.getProps();

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
            assert.equal(props.getHllPrecision(), 14);
            done();
        });

        it('should take a RpbSetBucketResp and call the users callback with the response', function(done) {
            // RpbSetBucketResp has no body. Riak just sends back the code so we supply null
            // to the command on success and a simple boolean true is sent to the user callback
            var callback = function(err, response) {
                assert.equal(response, true);
                done();
            };
            var storeProps = new StoreBucketTypeProps.Builder()
                .withBucketType('bucket_type')
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
           var storeProps = new StoreBucketTypeProps.Builder()
                .withBucketType('bucket_type')
                .withCallback(callback)
                .build();
            storeProps.onRiakError(rpbErrorResp);
        });
    });
});
