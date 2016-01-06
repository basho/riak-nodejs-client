'use strict';

var UpdateMap = require('../../../lib/commands/crdt/updatemap');
var MapField = require('../../../lib/protobuf/riakprotobuf').getProtoFor('MapField');
var MapUpdate = require('../../../lib/protobuf/riakprotobuf').getProtoFor('MapUpdate');
var DtUpdateResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('DtUpdateResp');
var MapEntry = require('../../../lib/protobuf/riakprotobuf').getProtoFor('MapEntry');
var ByteBuffer = require('bytebuffer');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');

var assert = require('assert');
var logger = require('winston');

describe('UpdateMap', function() {
    describe('UpdateMap', function() {
        it('should build a DtUpdateReq correctly', function(done) {
           var mapOp = new UpdateMap.MapOperation();

            mapOp.incrementCounter('counter_1', 50)
                .removeCounter('counter_2')
                .addToSet('set_1', 'set_value_1')
                .removeFromSet('set_2', 'set_value_2')
                .removeSet('set_3')
                .setRegister('register_1', new Buffer('register_value_1'))
                .removeRegister('register_2')
                .setFlag('flag_1', true)
                .removeFlag('flag_2')
                .removeMap('map_3');

            mapOp.map('map_2').incrementCounter('counter_1', 50)
                .removeCounter('counter_2')
                .addToSet('set_1', new Buffer('set_value_1'))
                .removeFromSet('set_2', new Buffer('set_value_2'))
                .removeSet('set_3')
                .setRegister('register_1', new Buffer('register_value_1'))
                .removeRegister('register_2')
                .setFlag('flag_1', true)
                .removeFlag('flag_2')
                .removeMap('map_3');

            var update = new UpdateMap.Builder()
                .withBucketType('maps')
                .withBucket('myBucket')
                .withKey('map_1')
                .withMapOperation(mapOp)
                .withContext(new Buffer('1234'))
                .withCallback(function(){})
                .withW(3)
                .withPw(1)
                .withDw(2)
                .withReturnBody(true)
                .withTimeout(20000)
                .build();

            var protobuf = update.constructPbRequest();

            assert.equal(protobuf.getType().toString('utf8'), 'maps');
            assert.equal(protobuf.getBucket().toString('utf8'), 'myBucket');
            assert.equal(protobuf.getKey().toString('utf8'), 'map_1');
            assert.equal(protobuf.getW(), 3);
            assert.equal(protobuf.getPw(), 1);
            assert.equal(protobuf.getDw(), 2);
            assert.equal(protobuf.getReturnBody(), true);
            assert.equal(protobuf.getTimeout(), 20000);
            assert.equal(protobuf.getContext().toString('utf8'), '1234');

            mapOp = protobuf.op.map_op;

            var verifyRemoves = function(removes) {
                assert.equal(removes.length, 5);
                var i;
                var counterRemoved = false;
                var setRemoved = false;
                var registerRemoved = false;
                var flagRemoved = false;
                var mapRemoved = false;
                for (i = 0; i < removes.length; i++ ) {
                    switch(removes[i].type) {
                        case MapField.MapFieldType.COUNTER:
                            assert.equal(removes[i].name.toString('utf8'), 'counter_2');
                            counterRemoved = true;
                            break;
                        case MapField.MapFieldType.SET:
                            assert.equal(removes[i].name.toString('utf8'), 'set_3');
                            setRemoved = true;
                            break;
                        case MapField.MapFieldType.MAP:
                            assert.equal(removes[i].name.toString('utf8'), 'map_3');
                            mapRemoved = true;
                            break;
                        case MapField.MapFieldType.REGISTER:
                            assert.equal(removes[i].name.toString('utf8'), 'register_2');
                            registerRemoved = true;
                            break;
                        case MapField.MapFieldType.FLAG:
                            assert.equal(removes[i].name.toString('utf8'), 'flag_2');
                            flagRemoved = true;
                            break;
                        default:
                            break;
                    }
                }

                assert(counterRemoved);
                assert(setRemoved);
                assert(registerRemoved);
                assert(flagRemoved);
                assert(mapRemoved);
            };

            var verifyUpdates = function(updates, expectMapUpdate) {
                var i;
                var counterIncremented = false;
                var setAddedTo = false;
                var setRemovedFrom = false;
                var registerSet = false;
                var flagSet = false;
                var mapAdded = false;
                var mapUpdate = null;
                for (i = 0; i < updates.length; i++) {
                    switch(updates[i].field.type) {
                        case MapField.MapFieldType.COUNTER:
                            assert.equal(updates[i].field.name.toString('utf8'), 'counter_1');
                            assert.equal(updates[i].counter_op.getIncrement(), 50);
                            counterIncremented = true;
                            break;
                        case MapField.MapFieldType.SET:
                            if (updates[i].set_op.adds.length) {
                                assert.equal(updates[i].field.name.toString('utf8'), 'set_1');
                                assert.equal(updates[i].set_op.adds[0].toString('utf8'),'set_value_1');
                                setAddedTo = true;

                            } else {
                                assert.equal(updates[i].field.name.toString('utf8'), 'set_2');
                                assert.equal(updates[i].set_op.removes[0].toString('utf8'),'set_value_2');
                                setRemovedFrom = true;
                            }
                            break;
                        case MapField.MapFieldType.MAP:
                            if (expectMapUpdate) {
                                assert.equal(updates[i].field.name.toString('utf8'), 'map_2');
                                mapAdded = true;
                                mapUpdate = updates[i];
                            }
                            break;
                        case MapField.MapFieldType.REGISTER:
                            assert.equal(updates[i].field.name.toString('utf8'), 'register_1');
                            assert.equal(updates[i].register_op.toString('utf8'), 'register_value_1');
                            registerSet = true;
                            break;
                        case MapField.MapFieldType.FLAG:
                            assert.equal(updates[i].field.name.toString('utf8'), 'flag_1');
                            assert.equal(updates[i].flag_op, MapUpdate.FlagOp.ENABLE);
                            flagSet = true;
                            break;
                        default:
                            break;
                    }
                }

                assert(counterIncremented);
                assert(setAddedTo);
                assert(setRemovedFrom);
                assert(registerSet);
                assert(flagSet);
                if (expectMapUpdate) {
                    assert(mapAdded);
                } else {
                    assert(!mapAdded);
                }

                return mapUpdate;
            };

            verifyRemoves(mapOp.removes);
            var innerMapUpdate = verifyUpdates(mapOp.updates, true);
            verifyRemoves(innerMapUpdate.map_op.removes);
            verifyUpdates(innerMapUpdate.map_op.updates, false);

            done();
        });

        it('should require context for any remove operations', function(done) {
           var mapOp = new UpdateMap.MapOperation();
            mapOp.incrementCounter('counter_1', 50)
                .removeCounter('counter_2')
                .addToSet('set_1', 'set_value_1')
                .removeFromSet('set_2', 'set_value_2')
                .removeSet('set_3')
                .setRegister('register_1', new Buffer('register_value_1'))
                .removeRegister('register_2')
                .setFlag('flag_1', true)
                .removeFlag('flag_2')
                .removeMap('map_3');
            mapOp.map('map_2').incrementCounter('counter_1', 50)
                .removeCounter('counter_2')
                .addToSet('set_1', new Buffer('set_value_1'))
                .removeFromSet('set_2', new Buffer('set_value_2'))
                .removeSet('set_3')
                .setRegister('register_1', new Buffer('register_value_1'))
                .removeRegister('register_2')
                .setFlag('flag_1', true)
                .removeFlag('flag_2')
                .removeMap('map_3');
            var b = new UpdateMap.Builder()
                .withBucketType('maps')
                .withBucket('myBucket')
                .withKey('map_1')
                .withMapOperation(mapOp)
                .withCallback(function(){});
            assert.throws(
                function () {
                    b.build();
                },
                function (err) {
                    assert.strictEqual(err.message,
                        'When doing any removes a context must be provided.');
                    return true;
                }
            );
            done();
        });

        it('should take a DtUpdateResp and call the users callback with the response', function(done) {
            var dtUpdateResp = new DtUpdateResp();
            dtUpdateResp.setKey(new Buffer('riak_generated_key'));
            dtUpdateResp.setContext(new Buffer('1234'));

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

            Array.prototype.push.apply(dtUpdateResp.map_value, createMapEntries());

            var mapEntry = new MapEntry();
            var mapField = new MapField();
            mapField.setType(MapField.MapFieldType.MAP);
            mapField.setName(new Buffer('map_1'));
            mapEntry.setField(mapField);
            Array.prototype.push.apply(mapEntry.map_value, createMapEntries());

            dtUpdateResp.map_value.push(mapEntry);

            var callback = function(err, resp) {
                assert(resp !== null);
                assert.equal(resp.generatedKey.toString('utf8'), 'riak_generated_key');
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

            var mapOp = new UpdateMap.MapOperation();

            var update = new UpdateMap.Builder()
                .withBucketType('maps')
                .withBucket('myBucket')
                .withKey('map_1')
                .withMapOperation(mapOp)
                .withCallback(callback)
                .build();

            update.onSuccess(dtUpdateResp);
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

            var mapOp = new UpdateMap.MapOperation();
            var update = new UpdateMap.Builder()
                .withBucketType('maps')
                .withBucket('myBucket')
                .withKey('map_1')
                .withMapOperation(mapOp)
                .withCallback(callback)
                .build();

            update.onRiakError(rpbErrorResp);
        });

    });

    describe('MapOperation', function() {
        it('should add counter increments', function(done) {
            var op = new UpdateMap.MapOperation();

            op.incrementCounter('counter_1', 5);
            op.incrementCounter('counter_2', 5);
            op.incrementCounter('counter_2', -10);

            assert.equal(op.counters.increment.length, 2);
            assert.equal(op.counters.increment[0].key, 'counter_1');
            assert.equal(op.counters.increment[0].increment, 5);
            assert.equal(op.counters.increment[1].key, 'counter_2');
            assert.equal(op.counters.increment[1].increment, -5);
            done();
        });

        it('should invalidate counter increment on remove', function(done) {
            var op = new UpdateMap.MapOperation();

            op.incrementCounter('counter_1', 5);
            op.removeCounter('counter_1');

            assert.equal(op.counters.increment.length, 0);
            assert.equal(op.counters.remove.length, 1);
            assert.equal(op.counters.remove[0], 'counter_1');
            assert(op._hasRemoves());
            done();
        });

        it('should invalidate counter remove on increment', function(done) {
            var op = new UpdateMap.MapOperation();

            op.removeCounter('counter_1');
            op.incrementCounter('counter_1', 5);
            assert.equal(op.counters.remove.length, 0);
            assert.equal(op.counters.increment[0].key, 'counter_1');
            assert.equal(op.counters.increment[0].increment, 5);
            done();
        });

        it('should add set adds', function(done) {
            var op = new UpdateMap.MapOperation();

            op.addToSet('set_1', 'value_1');
            op.addToSet('set_1', 'value_2');
            op.addToSet('set_2', 'value_1');
            assert.equal(op.sets.adds.length, 2);
            assert.equal(op.sets.adds[0].add.length, 2);
            assert.equal(op.sets.adds[0].key, 'set_1');
            assert.equal(op.sets.adds[0].add[0], 'value_1');
            assert.equal(op.sets.adds[0].add[1], 'value_2');
            assert.equal(op.sets.adds[1].add.length, 1);
            assert.equal(op.sets.adds[1].key, 'set_2');
            assert.equal(op.sets.adds[1].add[0], 'value_1');
            done();
        });

        it('should add set removes', function(done) {
            var op = new UpdateMap.MapOperation();

            op.removeFromSet('set_1', 'value_1');
            op.removeFromSet('set_1', 'value_2');
            op.removeFromSet('set_2', 'value_1');
            assert.equal(op.sets.removes.length, 2);
            assert.equal(op.sets.removes[0].remove.length, 2);
            assert.equal(op.sets.removes[0].key, 'set_1');
            assert.equal(op.sets.removes[0].remove[0], 'value_1');
            assert.equal(op.sets.removes[0].remove[1], 'value_2');
            assert.equal(op.sets.removes[1].remove.length, 1);
            assert.equal(op.sets.removes[1].key, 'set_2');
            assert.equal(op.sets.removes[1].remove[0], 'value_1');
            assert(op._hasRemoves());
            done();
        });

        it('should invalidate adding to a set on a remove set', function(done) {
           var op = new UpdateMap.MapOperation();

           op.addToSet('set_1', 'value_1');
           op.addToSet('set_2', 'value_1');
           op.removeSet('set_1');
           op.removeSet('set_3');

           assert.equal(op.sets.adds.length, 1);
           assert.equal(op.sets.adds[0].key, 'set_2');
           assert.equal(op.sets.remove.length, 2);
           assert.equal(op.sets.remove[0], 'set_1');
           assert.equal(op.sets.remove[1], 'set_3');
           assert(op._hasRemoves());
           done();
        });

        it('should invalidate removing from a set on a remove set', function(done) {
           var op = new UpdateMap.MapOperation();

           op.removeFromSet('set_1', 'value_1');
           op.removeFromSet('set_2', 'value_1');
           op.removeSet('set_1');
           op.removeSet('set_3');

           assert.equal(op.sets.removes.length, 1);
           assert.equal(op.sets.removes[0].key, 'set_2');
           assert.equal(op.sets.remove.length, 2);
           assert.equal(op.sets.remove[0], 'set_1');
           assert.equal(op.sets.remove[1], 'set_3');
           assert(op._hasRemoves());
           done();
        });

        it('should invalidate removing a set on an add to set', function(done) {
            var op = new UpdateMap.MapOperation();

            op.removeSet('set_1');
            op.removeSet('set_3');
            assert.equal(op.sets.remove.length, 2);
            op.addToSet('set_1', 'value_1');
            assert.equal(op.sets.remove.length, 1);
            assert.equal(op.sets.remove[0], 'set_3');
            assert.equal(op.sets.adds.length, 1);
            assert(op._hasRemoves());

            done();
        });

        it('should invalidate removing a set on a remove from set', function(done) {
            var op = new UpdateMap.MapOperation();

            op.removeSet('set_1');
            op.removeSet('set_3');
            assert.equal(op.sets.remove.length, 2);
            op.removeFromSet('set_1', 'value_1');
            assert.equal(op.sets.remove.length, 1);
            assert.equal(op.sets.remove[0], 'set_3');
            assert.equal(op.sets.removes.length, 1);
            assert(op._hasRemoves());

            done();
        });

        it('should add registers', function(done) {
           var op = new UpdateMap.MapOperation();

           op.setRegister('register_1', new Buffer('value_1'));
           op.setRegister('register_2', new Buffer('value_2'));

           assert.equal(op.registers.set.length, 2);
           assert.equal(op.registers.set[0].key, 'register_1');
           assert.equal(op.registers.set[0].value.toString('utf8'), 'value_1');
           assert.equal(op.registers.set[1].key, 'register_2');
           assert.equal(op.registers.set[1].value.toString('utf8'), 'value_2');

           op.setRegister('register_1', new Buffer('value_3'));
           assert.equal(op.registers.set[0].key, 'register_1');
           assert.equal(op.registers.set[0].value.toString('utf8'), 'value_3');
           done();
        });

        it('should invalidate register sets on removes', function(done) {
            var op = new UpdateMap.MapOperation();

            op.setRegister('register_1', new Buffer('value_1'));
            op.setRegister('register_2', new Buffer('value_2'));

            op.removeRegister('register_1');

            assert.equal(op.registers.set.length, 1);
            assert.equal(op.registers.set[0].key, 'register_2');
            assert.equal(op.registers.set[0].value.toString('utf8'), 'value_2');
            assert.equal(op.registers.remove.length, 1);
            assert.equal(op.registers.remove[0], 'register_1');
            assert(op._hasRemoves());
            done();
        });

        if('should invalidate register removes on register sets', function(done) {
            var op = new UpdateMap.MapOperation();
            op.removeRegister('register_1');
            op.removeRegister('register_2');
            assert.equal(op.registers.remove.length, 2);
            assert.equal(op.registers.remove[0], 'register_1');
            assert.equal(op.registers.remove[1], 'register_2');

            op.setRegister('register_1', new Buffer('value_1'));
            assert.equal(op.registers.set.length, 1);
            assert.equal(op.registers.remove.length, 1);
            assert.equal(op.registers.remove[0], 'register_2');
            assert(op._hasRemoves());
            done();
        });

        it('should set flags', function(done) {
            var op = new UpdateMap.MapOperation();

            op.setFlag('flag_1', true);
            op.setFlag('flag_2', false);

            assert.equal(op.flags.set.length, 2);
            assert.equal(op.flags.set[0].key, 'flag_1');
            assert.equal(op.flags.set[1].key, 'flag_2');
            assert.equal(op.flags.set[0].state, true);
            assert.equal(op.flags.set[1].state, false);

            op.setFlag('flag_1', false);
            assert.equal(op.flags.set.length, 2);
            assert.equal(op.flags.set[0].state, false);
            done();
        });

        it('should invalidate a flag set on a flag remove', function(done) {
            var op = new UpdateMap.MapOperation();

            op.setFlag('flag_1', true);
            op.setFlag('flag_2', false);
            op.removeFlag('flag_2');
            assert.equal(op.flags.set.length, 1);
            assert.equal(op.flags.set[0].key, 'flag_1');
            assert.equal(op.flags.remove.length, 1);
            assert.equal(op.flags.remove[0], 'flag_2');
            assert(op._hasRemoves());
            done();
        });

        it('should invalidate a remove flag on a set flag', function(done) {
            var op = new UpdateMap.MapOperation();

            op.removeFlag('flag_1');
            op.removeFlag('flag_2');
            assert.equal(op.flags.remove.length, 2);
            op.setFlag('flag_1', true);
            assert.equal(op.flags.remove.length, 1);
            assert.equal(op.flags.remove[0], 'flag_2');
            assert.equal(op.flags.set.length, 1);
            assert(op._hasRemoves());
            done();
        });

        it('should nest maps', function(done) {
            var op = new UpdateMap.MapOperation();
            op.map('map_1')
                    .setFlag('some_flag', true)
                    .incrementCounter('counter_1', 5)
                    .removeRegister('register_1');

            assert.equal(op.counters.increment.length, 0);
            assert.equal(op.flags.set.length, 0);
            assert.equal(op.registers.remove.length, 0);
            assert.equal(op.maps.modify.length, 1);

            assert.equal(op.maps.modify[0].map.counters.increment.length, 1);
            assert.equal(op.maps.modify[0].map.flags.set.length, 1);
            assert.equal(op.maps.modify[0].map.registers.remove.length, 1);
            assert(op._hasRemoves());
            done();
        });

        it('should detect removes in deeply nested maps', function(done) {
            var op = new UpdateMap.MapOperation();
            var map_1 = op.map('map_1');
            map_1.incrementCounter('counter_1', 5);

            var depth_1 = map_1.map('depth_1');
            depth_1.incrementCounter('depth_1_counter_1', 5);

            var depth_2 = depth_1.map('depth_2');
            depth_2.incrementCounter('depth_2_counter_1', 5);
            depth_2.removeRegister('depth_2_register_1');

            assert.equal(op.counters.increment.length, 0);
            assert.equal(op.flags.set.length, 0);
            assert.equal(op.registers.remove.length, 0);
            assert.equal(op.maps.modify.length, 1);

            logger.debug("[unit/crdt/updatemap] op.maps %s", JSON.stringify(op.maps));

            var map_1_modify = op.maps.modify[0].map;
            assert.equal(map_1_modify.counters.increment.length, 1);

            var depth_1_modify = map_1_modify.maps.modify[0].map;
            assert.equal(depth_1_modify.counters.increment.length, 1);

            var depth_2_modify = depth_1_modify.maps.modify[0].map;
            assert.equal(depth_2_modify.counters.increment.length, 1);
            assert.equal(depth_2_modify.registers.remove.length, 1);

            assert(op._hasRemoves());

            done();
        });

        it('should invalidate a map modify on a map remove', function(done) {
            var op = new UpdateMap.MapOperation();
            op.map('map_1').setFlag('some_flag', true);

            op.removeMap('map_1');
            assert.equal(op.maps.modify.length, 0);
            assert.equal(op.maps.remove.length, 1);
            assert(op._hasRemoves());
            assert.equal(op.maps.remove[0], 'map_1');
            done();
        });

        it('should invalidate a map remove on a map modify', function(done) {
            var op = new UpdateMap.MapOperation();
            op.removeMap('map_1');
            op.removeMap('map_2');
            assert.equal(op.maps.remove.length, 2);
            op.map('map_1').setFlag('some_flag', true);
            assert.equal(op.maps.remove.length, 1);
            assert.equal(op.maps.modify.length, 1);
            assert(op._hasRemoves());
            done();
        });
    });
});
