﻿/*
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

var assert = require('assert');
var logger = require('winston');

var Riak = require('../../lib/client');
var CommandBase = require('../../lib/commands/commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');

function TestCommand(options, callback) {
    CommandBase.call(this, 'RpbGetReq', 'RpbGetResp', callback);
    var self = this;
    Joi.validate(options, schema, function(err, options) {

        if (err) {
            throw err;
        }

        self.options = options;
    });
}

inherits(TestCommand, CommandBase);

var schema = Joi.object().keys({
   bucketType: Joi.string().default('default'),
   bucket: Joi.string().required(),
   key: Joi.binary().required()
});

function TestBuilder() {}

TestBuilder.prototype = {
    withBucket : function(bucket) {
        this.bucket = bucket;
        return this;
    },
    withKey : function(key) {
        this.key = key;
        return this;
    },
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new TestCommand(this, cb);
    }
};

describe('CommandBase', function() {

    function cb(err, rslt) { }

    describe('TestCommand', function() {

        it('should use callback passed as ctor argument', function(done) {
            var options = {
                bucketType: 'maps',
                bucket: 'foo',
                key: 'bar'
            };
            assert.doesNotThrow(function () {
                var cmd = new TestCommand(options, cb);
                assert(cmd.callback == cb);
                done();
            });
        });

        it('should use callback via Builder', function(done) {
            assert.doesNotThrow(function () {
                var cmd = new TestBuilder()
                    .withBucket('foo')
                    .withKey('bar')
                    .withCallback(cb)
                    .build();
                assert(cmd.callback == cb);
                done();
            });
        });

        it('should throw when callback not passed to ctor', function(done) {
            var options = {
                bucketType: 'maps',
                bucket: 'foo',
                key: 'bar'
            };
            assert.throws(function () {
                var cmd = new TestCommand(options);
            });
            done();
        });

        it('should throw when callback passed via options', function(done) {
            var options = {
                bucketType: 'maps',
                bucket: 'foo',
                key: 'bar',
                callback: cb
            };
            assert.throws(function () {
                var cmd = new TestCommand(options, cb);
            });
            done();
        });

    });

    describe('Commands', function() {
        var default_options = {
            bucketType: 'baz',
            bucket: 'foo',
            key: 'bar',
            callback: cb
        };

        var default_builder_func = function (b) {
            b.withBucketType(default_options.bucketType);
            b.withBucket(default_options.bucket);
            b.withKey(default_options.key);
        };

        var updateMapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();

        var commands = {
            'YZ.DeleteIndex' : {
                    options : {
                        indexName: 'frazzle',
                        callback: cb
                    },
                    builder_func: function (b) {
                        b.withIndexName('frazzle');
                    }
                },
            'YZ.FetchIndex' : {
                    options : {
                        indexName: 'frazzle',
                        callback: cb
                    },
                    builder_func: function (b) {
                        b.withIndexName('frazzle');
                    }
                },
            'YZ.FetchSchema' : {
                    options : {
                        schemaName: 'frazzle',
                        callback: cb
                    },
                    builder_func: function (b) {
                        b.withSchemaName('frazzle');
                    }
                },
            'YZ.Search' : {
                    options : {
                        q : '*:*',
                        indexName: 'frazzle',
                        callback: cb
                    },
                    builder_func: function (b) {
                        b.withIndexName('frazzle');
                        b.withQuery('*:*');
                    }
                },
            'YZ.StoreIndex' : {
                    options : {
                        indexName: 'frazzle',
                        callback: cb
                    },
                    builder_func: function (b) {
                        b.withIndexName('frazzle');
                    }
                },
            'YZ.StoreSchema' : {
                    options : {
                        schemaName: 'frazzle',
                        schema: '<fauxml></fauxml>',
                        callback: cb
                    },
                    builder_func: function (b) {
                        b.withSchemaName('frazzle');
                        b.withSchema('<fauxml></fauxml>');
                    }
                },
            'CRDT.FetchCounter' : {
                    options : default_options,
                    builder_func : default_builder_func
                },
            'CRDT.FetchMap' : {
                    options : default_options,
                    builder_func : default_builder_func
                },
            'CRDT.FetchSet' : {
                    options : default_options,
                    builder_func : default_builder_func
                },
            'CRDT.UpdateCounter' : {
                    options : {
                        bucketType: default_options.bucketType,
                        bucket: default_options.bucket,
                        key: default_options.key,
                        increment: 1,
                        callback: cb
                    },
                    builder_func: function (b) {
                        b.withBucketType(default_options.bucketType);
                        b.withBucket(default_options.bucket);
                        b.withKey(default_options.key);
                        b.withIncrement(1);
                    }
                },
            'CRDT.UpdateMap' : {
                    options : {
                        bucketType: default_options.bucketType,
                        bucket: default_options.bucket,
                        key: default_options.key,
                        op: updateMapOp,
                        callback: cb
                    },
                    builder_func: function (b) {
                        b.withBucketType(default_options.bucketType);
                        b.withBucket(default_options.bucket);
                        b.withKey(default_options.key);
                        b.withMapOperation(updateMapOp);
                    }
                },
            'CRDT.UpdateSet' : {
                    options : default_options,
                    builder_func : default_builder_func
                },
            'KV.DeleteValue' : {
                    options : default_options,
                    builder_func: function (b) {
                        b.withBucket(default_options.bucket);
                        b.withKey(default_options.key);
                    }
                },
            'KV.FetchBucketProps' : {
                    options : {
                        bucketType: default_options.bucketType,
                        bucket: default_options.bucket,
                        callback: cb
                    },
                    builder_func: function (b) {
                        b.withBucket(default_options.bucket);
                    }
                },
            'KV.FetchValue' : {
                    options : default_options,
                    builder_func: function (b) {
                        b.withBucket(default_options.bucket);
                        b.withKey(default_options.key);
                    }
                },
            'KV.ListBuckets' : {
                    options : {
                        bucketType: default_options.bucketType,
                        callback: cb
                    }
                },
            'KV.ListKeys' : {
                    options : {
                        bucketType: default_options.bucketType,
                        bucket: default_options.bucket,
                        callback: cb
                    },
                    builder_func: function (b) {
                        b.withBucket(default_options.bucket);
                    }
                },
            'KV.SecondaryIndexQuery' : {
                    options : {
                        bucketType: default_options.bucketType,
                        bucket: default_options.bucket,
                        indexName: 'frazzle_bin',
                        indexKey: '12345',
                        callback: cb
                    },
                    builder_func: function (b) {
                        b.withBucketType(default_options.bucketType);
                        b.withBucket(default_options.bucket);
                        b.withIndexName('frazzle_bin');
                        b.withIndexKey('12345');
                    }
                },
            'KV.StoreBucketProps' : {
                    options : {
                        bucketType: default_options.bucketType,
                        bucket: default_options.bucket,
                        callback: cb
                    },
                    builder_func: function (b) {
                        b.withBucket(default_options.bucket);
                    }
                },
            'KV.StoreValue' : {
                    options : {
                        bucketType: default_options.bucketType,
                        bucket: default_options.bucket,
                        key: default_options.key,
                        value: 'blargh',
                        callback: cb
                    },
                    builder_func : function (b) {
                        b.withBucketType(default_options.bucketType);
                        b.withBucket(default_options.bucket);
                        b.withKey(default_options.key);
                        b.withContent('blargh');
                    }
                },
        };

        it('should throw when callback passed via options', function(done) {
            Object.keys(commands).forEach(function (cmd_name) {
                var options = commands[cmd_name].options;
                var eval_str = "new Riak.Commands." + cmd_name + "(options, cb)";
                var e_message = null;
                try {
                    var cmd = eval(eval_str); // jshint ignore:line
                } catch (e) {
                    if (e.message !== '"callback" is not allowed') {
                        logger.error("%s ctor threw: %s", cmd_name, e.message);
                        throw e;
                    }
                }
            });
            done();
        });

        it('should use callback via Builder', function(done) {
            Object.keys(commands).forEach(function (cmd_name) {
                var options = commands[cmd_name].options;
                var builder_func = commands[cmd_name].builder_func;

                var eval_str = "new Riak.Commands." + cmd_name + ".Builder()";
                var builder = eval(eval_str); // jshint ignore:line
                builder.withCallback(cb);

                if (builder_func) {
                    builder_func(builder);
                }

                var cmd;
                try {
                    cmd = builder.build();
                } catch (e) {
                    logger.error("%s builder.build() threw: %s", cmd_name, e.message);
                    throw e;
                }
                assert(cmd.callback == cb);
            });
            done();
        });

    });

});

