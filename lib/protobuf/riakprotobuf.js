/*
 * Copyright 2014 Basho Technologies, Inc.
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

var ProtoBuf = require('protobufjs');
var HashMap = require('hashmap').HashMap;
var fs  = require('fs');

var builder = ProtoBuf.newBuilder();
ProtoBuf.loadProtoFile(__dirname + '/riak.proto', builder);
ProtoBuf.loadProtoFile(__dirname + '/riak_dt.proto', builder);
ProtoBuf.loadProtoFile(__dirname + '/riak_kv.proto', builder);
ProtoBuf.loadProtoFile(__dirname + '/riak_search.proto', builder);
ProtoBuf.loadProtoFile(__dirname + '/riak_yokozuna.proto', builder);

var codeMap = new HashMap();
fs.readFileSync(__dirname + '/riak_pb_messages.csv').toString().split('\n').forEach(function(line) {
    if (line === '') return;
    var array = line.split(',');
    codeMap.set(parseInt(array[0]), array[1]);
    codeMap.set(array[1], parseInt(array[0]));
});

module.exports = {
    getProtoFor : function(nameOrNumber) {
        if (typeof nameOrNumber === 'number') {
            nameOrNumber = codeMap.get(nameOrNumber);
        }
        return builder.build(nameOrNumber);
    },
    getCodeFor : function(name) {
        return codeMap.get(name);
    }
};

