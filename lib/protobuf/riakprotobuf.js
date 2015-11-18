'use strict';

var ProtoBuf = require('protobufjs');
var HashMap = require('hashmap').HashMap;
var fs  = require('fs');
var path  = require('path');

var riakPbSrc = path.join(__dirname, 'riak_pb', 'src');

var builder = ProtoBuf.newBuilder();
ProtoBuf.loadProtoFile(path.join(riakPbSrc, 'riak.proto'), builder);
ProtoBuf.loadProtoFile(path.join(riakPbSrc, 'riak_dt.proto'), builder);
ProtoBuf.loadProtoFile(path.join(riakPbSrc, 'riak_kv.proto'), builder);
ProtoBuf.loadProtoFile(path.join(riakPbSrc, 'riak_ts.proto'), builder);
ProtoBuf.loadProtoFile(path.join(riakPbSrc, 'riak_search.proto'), builder);
ProtoBuf.loadProtoFile(path.join(riakPbSrc, 'riak_yokozuna.proto'), builder);

var codeMap = new HashMap();
fs.readFileSync(path.join(riakPbSrc, 'riak_pb_messages.csv')).toString().split('\n').forEach(function(line) {
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
