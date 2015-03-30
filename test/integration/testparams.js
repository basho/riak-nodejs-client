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
var logger = require('winston');
/*
 * To increase logging verbosity: */

logger.remove(logger.transports.Console);
logger.add(logger.transports.Console, {
    level : 'debug',
    colorize: true,
    timestamp: true
});
// */


module.exports.bucketName = 'riak_index_tests';

/**
* Bucket type
* 
* you must create the type 'plain' to use this:
*
* riak-admin bucket-type create plain '{"props":{}}'
* riak-admin bucket-type activate plain
*/
module.exports.bucketType = 'leveldb_type';

module.exports.nodeAddresses = ['riak-test:10017'];

module.exports.cleanBucket = function(cluster, type, bucket, callback) {
  
    // Note this also acts as the integration test for ListKeys and 
    // DeleteValue
    logger.debug('Clearing bucket: %s:%s', type, bucket);
    var numKeys = 0;
    var count = 0;
    var lkCallback = function(err, resp) {
        assert(!err, err);
        
        numKeys += resp.keys.length;

        if (numKeys > 0) {
            logger.debug('\tkey count to delete %s', numKeys);
        } else {
            logger.debug('DONE clearing bucket: %s:%s', type, bucket);
            callback();
        }
        
        var dCallback = function(err, resp) {
            assert(!err, err);
            count++;
            if (count === numKeys) {
                logger.debug('DONE clearing bucket: %s:%s', type, bucket);
                callback();
            }
        };
        
        for (var i = 0; i < resp.keys.length; i++) {
            
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

