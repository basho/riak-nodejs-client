'use strict';

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

var FetchMap = require('../../../lib/commands/crdt/fetchmap');
var MapField = require('../../../lib/protobuf/riakprotobuf').getProtoFor('MapField');
var MapUpdate = require('../../../lib/protobuf/riakprotobuf').getProtoFor('MapUpdate');
var DtFetchResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('DtFetchResp');
var MapEntry = require('../../../lib/protobuf/riakprotobuf').getProtoFor('MapEntry');
var DtValue = require('../../../lib/protobuf/riakprotobuf').getProtoFor('DtValue');
var ByteBuffer = require('bytebuffer');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');

var assert = require('assert');

describe('FetchMap', function() {
    
    describe('Build', function() {
    
        it('should build a DtFetchReq correctly', function(done) {
            
            var fetch = new FetchMap.Builder()
                .withBucketType('maps')
                .withBucket('myBucket')
                .withKey('map_1')
                .withCallback(function(){})
                .withR(1)
                .withPr(2)
                .withNotFoundOk(true)
                .withBasicQuorum(true)
                .withTimeout(20000)
                .build();
        
            var protobuf = fetch.constructPbRequest();
            
            assert.equal(protobuf.getType().toString('utf8'), 'maps');
            assert.equal(protobuf.getBucket().toString('utf8'), 'myBucket');
            assert.equal(protobuf.getKey().toString('utf8'), 'map_1');
            assert.equal(protobuf.getR(), 1);
            assert.equal(protobuf.getPr(), 2);
            assert.equal(protobuf.getNotfoundOk(), true);
            assert.equal(protobuf.getBasicQuorum(), true);
            assert.equal(protobuf.getTimeout(), 20000);
            done();
            
        });
        
        it('should take a DtFetchResp and call the users callback with the response', function(done) {
           
            var dtFetchResp = new DtFetchResp();
            
            dtFetchResp.setType(DtFetchResp.DataType.MAP);
            dtFetchResp.setContext(new Buffer('1234'));
            var dtValue = new DtValue();
            var createMapEntries = function() {
                var mapEntries = [];
                
                var mapEntry = new MapEntry();
                var mapField = new MapField();
                mapField.setType(MapField.MapFieldType.COUNTER);
                mapField.setName(new Buffer('counter_1'));
                mapEntry.setField(mapField);
                mapEntry.setCounterValue(50);
                mapEntries.push(mapEntry);
                
                mapEntry = new MapEntry();
                mapField = new MapField();
                mapField.setType(MapField.MapFieldType.SET);
                mapField.setName(new Buffer('set_1'));
                mapEntry.setField(mapField);
                Array.prototype.push.apply(mapEntry.set_value, [ByteBuffer.fromUTF8('value_1'), ByteBuffer.fromUTF8('value_2')]);
                mapEntries.push(mapEntry);
                
                mapEntry = new MapEntry();
                mapField = new MapField();
                mapField.setType(MapField.MapFieldType.REGISTER);
                mapField.setName(new Buffer('register_1'));
                mapEntry.setField(mapField);
                mapEntry.setRegisterValue(ByteBuffer.fromUTF8('1234'));
                mapEntries.push(mapEntry);
                
                mapEntry = new MapEntry();
                mapField = new MapField();
                mapField.setType(MapField.MapFieldType.FLAG);
                mapField.setName(new Buffer('flag_1'));
                mapEntry.setField(mapField);
                mapEntry.setFlagValue(true);
                mapEntries.push(mapEntry);
                
                return mapEntries;

            };
            
            Array.prototype.push.apply(dtValue.map_value, createMapEntries());
            
            var mapEntry = new MapEntry();
            var mapField = new MapField();
            mapField.setType(MapField.MapFieldType.MAP);
            mapField.setName(new Buffer('map_1'));
            mapEntry.setField(mapField);
            Array.prototype.push.apply(mapEntry.map_value, createMapEntries());
            
            dtValue.map_value.push(mapEntry);
            
            dtFetchResp.setValue(dtValue);
            
            var callback = function(err, resp) {
                assert(resp !== null);
                assert.equal(resp.context.toString('utf8'), '1234');
                
                var verifyMap = function(map) {
                    assert.equal(map.counters.counter_1, 50);
                    assert.equal(map.sets.set_1[0], 'value_1');
                    assert.equal(map.sets.set_1[1], 'value_2');
                    assert.equal(map.registers.register_1.toString('utf8'), '1234');
                    assert.equal(map.flags.flag_1, true);
                };
            
                verifyMap(resp.map);
                verifyMap(resp.map.maps.map_1);
                done();
            
            };
            
            var fetch = new FetchMap.Builder()
                .withBucketType('maps')
                .withBucket('myBucket')
                .withKey('map_1')
                .withCallback(callback)
                .build();
        
            fetch.onSuccess(dtFetchResp);
            
            
        });
        
    });
    
    
    
});
