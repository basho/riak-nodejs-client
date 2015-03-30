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

var ListKeys = require('../../lib/commands/kv/listkeys');
var DeleteValue = require('../../lib/commands/kv/deletevalue');
var assert = require('assert');

/*
 * To increase logging verbosity: */
var logger = require('winston');
logger.remove(logger.transports.Console);
logger.add(logger.transports.Console, {
    level : 'debug',
    colorize: true,
    timestamp: true
});
// */


module.exports.bucketName = 'njstest_test_bucket';

/**
* Bucket type
* 
* you must create the type 'njstest_test_type' to use this:
*
* riak-admin bucket-type create njstest_test_type '{"props":{}}'
* riak-admin bucket-type activate njstest_test_type
*/
module.exports.bucketType = 'njstest_test_type';

module.exports.nodeAddresses = ['127.0.0.1:8087'];

module.exports.cleanBucket = function(cluster, type, bucket, callback) {
  
    // Note this also acts as the integration test for ListKeys and 
    // DeleteValue
    var numKeys = 0;
    var count = 0;
    var lkCallback = function(err, resp) {
        assert(!err, err);
        var i;
        
        numKeys += resp.keys.length;
        
        var dCallback = function(err, resp) {
            assert(!err, err);
            count++;
            if (count === numKeys) {
                callback();
            }
        
        };
        
        for (i = 0; i < resp.keys.length; i++) {
            
            var del = new DeleteValue.Builder()
                    .withBucket(resp.bucket)
                    .withBucketType(resp.bucketType)
                    .withKey(resp.keys[i])
                    .withCallback(dCallback)
                    .build();
            
            cluster.execute(del);
        }
        
    };
    
    var list = new ListKeys.Builder()
            .withBucket(bucket)
            .withBucketType(type)
            .withCallback(lkCallback)
            .withStreaming(false)
            .build();
    
    cluster.execute(list);
    
    
};