'use strict';

var rpb = require('../../../lib/protobuf/riakprotobuf');
var FetchBucketProps = require('../../../lib/commands/kv/fetchbucketprops');
var RpbGetBucketResp = rpb.getProtoFor('RpbGetBucketResp');
var RpbBucketProps = rpb.getProtoFor('RpbBucketProps');
var RpbErrorResp = rpb.getProtoFor('RpbErrorResp');
var RpbCommitHook = rpb.getProtoFor('RpbCommitHook');
var RpbModFun = rpb.getProtoFor('RpbModFun');
var assert = require('assert');

describe('FetchBucketProps', function() {
    describe('Build', function() {
        it('should build a RpbGetBucketProps correctly', function(done) {
            var bucketType = 'bucket_type';
            var bucketName = 'bucket_name';

            var opts = {
                bucketType: bucketType,
                bucket: bucketName,
            };
            var cb = function (err, rslt) {};
            var fetchProps = new FetchBucketProps(opts, cb);

            var protobuf = fetchProps.constructPbRequest();
            assert.equal(protobuf.getType().toString('utf8'), bucketType);
            assert.equal(protobuf.getBucket().toString('utf8'), bucketName);

            var fetchPropsBuilder = new FetchBucketProps.Builder();

            fetchPropsBuilder.withBucketType(bucketType);
            fetchPropsBuilder.withBucket(bucketName);
            fetchPropsBuilder.withCallback(cb);

            fetchProps = fetchPropsBuilder.build();

            protobuf = fetchProps.constructPbRequest();
            assert.equal(protobuf.getType().toString('utf8'), bucketType);
            assert.equal(protobuf.getBucket().toString('utf8'), bucketName);
            done();
        });
        
        it('should take a RpbGetBucketResp and call the users callback with the response', function(done) {
            var props = new RpbBucketProps();
            
            props.setNVal(3);
            props.setAllowMult(true);
            props.setLastWriteWins(true);
            props.setHasPrecommit(true);
            props.setHasPostcommit(true);
            props.setOldVclock(86400);
            props.setYoungVclock(20);
            props.setBigVclock(50);
            props.setSmallVclock(50);
            props.setR(1);
            props.setPr(2);
            props.setW(3);
            props.setPw(4);
            props.setDw(5);
            props.setRw(6);
            props.setBasicQuorum(true);
            props.setNotfoundOk(true);
            props.setSearch(true);
            props.setConsistent(true);
            props.setRepl(1);
            props.setBackend(new Buffer('backend'));
            props.setSearchIndex(new Buffer('index'));
            props.setDatatype(new Buffer('dataType'));
            
            var hook = new RpbCommitHook();
            var modfun = new RpbModFun();
            modfun.module = new Buffer('module_name');
            modfun.function = new Buffer('function_name');
            hook.modfun = modfun;
            
            props.precommit.push(hook);
            props.postcommit.push(hook);
            props.setChashKeyfun(modfun);
            props.setLinkfun(modfun);
            
            var protobuf = new RpbGetBucketResp();
            protobuf.setProps(props);
            
            var callback = function(err, response) {
                assert.equal(response.nVal, 3);
                assert.equal(response.allowMult, true);
                assert.equal(response.lastWriteWins, true);
                assert.equal(response.hasPrecommit, true);
                assert.equal(response.hasPostcommit, true);
                assert.equal(response.oldVClock, 86400);
                assert.equal(response.youngVClock, 20);
                assert.equal(response.bigVClock, 50);
                assert.equal(response.smallVClock, 50);
                assert.equal(response.r, 1);
                assert.equal(response.pr, 2);
                assert.equal(response.w, 3);
                assert.equal(response.pw, 4);
                assert.equal(response.dw, 5);
                assert.equal(response.rw, 6);
                assert.equal(response.basicQuorum, true);
                assert.equal(response.notFoundOk, true);
                assert.equal(response.search, true);
                assert.equal(response.consistent, true);
                assert.equal(response.repl, 1);
                assert.equal(response.backend, 'backend');
                assert.equal(response.searchIndex, 'index');
                assert.equal(response.dataType, 'dataType');
                assert.equal(response.precommit[0].mod, 'module_name');
                assert.equal(response.precommit[0].fun, 'function_name');
                assert.equal(response.postcommit[0].mod, 'module_name');
                assert.equal(response.postcommit[0].fun, 'function_name');
                assert.equal(response.chashKeyfun.mod, 'module_name');
                assert.equal(response.chashKeyfun.fun, 'function_name');
                assert.equal(response.linkFun.mod, 'module_name');
                assert.equal(response.linkFun.fun, 'function_name');
                done();
            };
            
            var fetchProps = new FetchBucketProps.Builder()
                    .withBucketType('bucket_type')
                    .withBucket('bucket_name')
                    .withCallback(callback)
                    .build();
            fetchProps.onSuccess(protobuf);
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
           
           var fetchCommand = new FetchBucketProps.Builder()
               .withBucketType('bucket_type')
               .withBucket('bucket_name')
               .withCallback(callback)
               .build();
            fetchCommand.onRiakError(rpbErrorResp);
       });
    });
});
