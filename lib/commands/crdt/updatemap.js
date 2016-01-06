'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');

var utils = require('../../utils');
var rpb = require('../../protobuf/riakprotobuf');
var DtOp = rpb.getProtoFor('DtOp');
var MapOp = rpb.getProtoFor('MapOp');
var MapField = rpb.getProtoFor('MapField');
var MapUpdate = rpb.getProtoFor('MapUpdate');
var CounterOp = rpb.getProtoFor('CounterOp');
var SetOp = rpb.getProtoFor('SetOp');
var FlagOp = rpb.getProtoFor('FlagOp');
var FetchMap = require('./fetchmap');

/**
 * Provides the UpdateMap class, its builders, and its response.
 * @module CRDT
 */

/**
 * Command used to update a Map in Riak.
 *
 * As a convenience, a builder method is provided as well as an object with
 * a fluent API for constructing the update.
 *
 *     var mapOp = new UpdateMap.MapOperation();
 *     mapOp.incrementCounter('counter_1', 50)
 *         .addToSet('set_1', 'set_value_1')
 *         .setRegister('register_1', new Buffer('register_value_1'))
 *         .setFlag('flag_1', true)
 *         .map('inner_map')
 *             .incrementCounter('counter_1', 50)
 *             .addToSet('set_2', 'set_value_2');
 *
 * See {{#crossLink "UpdateMap.MapOperation"}}UpdateMap.MapOperation{{/crossLink}}
 *
 *     var update = new UpdateMap.Builder()
 *               .withBucketType('maps')
 *               .withBucket('myBucket')
 *               .withKey('map_1')
 *               .withMapOperation(mapOp)
 *               .withCallback(myCallback)
 *               .withReturnBody(true)
 *               .build();
 *
 * See {{#crossLink "UpdateMap.Builder"}}UpdateMap.Builder{{/crossLink}}
 * @class UpdateMap
 * @constructor
 * @param {Object} options The options to use for this command.
 * @param {String} options.bucketType The bucket type in riak.
 * @param {String} options.bucket The bucket in riak.
 * @param {MapOperation} options.op The set of modifications to make to this map.
 * @param {String} [options.key] The key for the counter you want to store. If not supplied Riak will gererate one.
 * @param {Buffer} [options.context] The context from a previous fetch. Required for remove operations.
 * @param {Number} [options.w] The W value to use.
 * @param {Number} [options.dw] The DW value to use.
 * @param {Number} [options.pw] The PW value to use.
 * @param {Boolean} [options.returnBody=true] Return the map.
 * @param {Boolean} [options.setsAsBuffers=false] Return sets as arrays of Buffers rather than strings.
 * @param {Number} [options.timeout] Set a timeout for this command.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response The response from Riak. Will be null if returnBody is not set.
 * @param {String} callback.response.generatedKey If no key was supplied, Riak will generate and return one here.
 * @param {Buffer} callback.response.context An opaque context to be used in any subsequent modification of the map.
 * @param {Object} callback.response.map The map in Riak, converted to a JS object.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function UpdateMap(options, callback) {
    CommandBase.call(this, 'DtUpdateReq', 'DtUpdateResp', callback);
    this.validateOptions(options, schema);
    if (this.options.op._hasRemoves() && this.options.context === null) {
        throw new Error('When doing any removes a context must be provided.');
    }
}

inherits(UpdateMap, CommandBase);

UpdateMap.prototype.constructPbRequest = function() {

    var protobuf = this.getPbReqBuilder();

    protobuf.setBucket(new Buffer(this.options.bucket));
    protobuf.setType(new Buffer(this.options.bucketType));
    // key can be null to have Riak generate it.
    if (this.options.key) {
        protobuf.setKey(new Buffer(this.options.key));
    }

    protobuf.setTimeout(this.options.timeout);
    protobuf.setW(this.options.w);
    protobuf.setPw(this.options.pw);
    protobuf.setDw(this.options.dw);
    protobuf.setReturnBody(this.options.returnBody);

    protobuf.setContext(this.options.context);

    var dtOp = new DtOp();

    var pbMapOp = new MapOp();

    this._populate(this.options.op, pbMapOp);

    dtOp.setMapOp(pbMapOp);

    protobuf.setOp(dtOp);

    return protobuf;

};

UpdateMap.prototype.onSuccess = function(dtUpdateResp) {
    // dtUpdateResp will be null if returnBody wasn't specified
    var response = null;
    // on "not found" dtFetchResp will be null
    if (dtUpdateResp) {
        var key = null;
        if (dtUpdateResp.key) {
            key = dtUpdateResp.key.toString('utf8');
        }
        response = { generatedKey: key,
                context: dtUpdateResp.getContext().toBuffer(),
                map: FetchMap.parsePbResponse(dtUpdateResp.map_value, this.options.setsAsBuffers) };

    }
    this._callback(null, response);
    return true;

};

UpdateMap.prototype._populate = function(mapOp, pbMapOp) {
    function maybeConvertStringToBuffer(value) {
        var rv = value;
        if (utils.isString(value)) {
            rv = new Buffer(value);
        }
        return rv;
    }

    var i;
    var field;
    var update;
    if (mapOp._hasRemoves()) {
        for (i = 0; i < mapOp.counters.remove.length; i++) {
            field = new MapField();
            field.setName(new Buffer(mapOp.counters.remove[i]));
            field.setType(MapField.MapFieldType.COUNTER);
            pbMapOp.removes.push(field);
        }
        for (i = 0; i < mapOp.sets.remove.length; i++) {
            field = new MapField();
            field.setName(new Buffer(mapOp.sets.remove[i]));
            field.setType(MapField.MapFieldType.SET);
            pbMapOp.removes.push(field);
        }
        for (i = 0; i < mapOp.maps.remove.length; i++) {
            field = new MapField();
            field.setName(new Buffer(mapOp.maps.remove[i]));
            field.setType(MapField.MapFieldType.MAP);
            pbMapOp.removes.push(field);
        }
        for (i = 0; i < mapOp.registers.remove.length; i++) {
            field = new MapField();
            field.setName(new Buffer(mapOp.registers.remove[i]));
            field.setType(MapField.MapFieldType.REGISTER);
            pbMapOp.removes.push(field);
        }
        for (i = 0; i < mapOp.flags.remove.length; i++) {
            field = new MapField();
            field.setName(new Buffer(mapOp.flags.remove[i]));
            field.setType(MapField.MapFieldType.FLAG);
            pbMapOp.removes.push(field);
        }
    }

    for (i = 0; i < mapOp.counters.increment.length; i++) {
        update = new MapUpdate();
        field = new MapField();
        field.setName(new Buffer(mapOp.counters.increment[i].key));
        field.setType(MapField.MapFieldType.COUNTER);
        var counterOp = new CounterOp();
        counterOp.setIncrement(mapOp.counters.increment[i].increment);
        update.setField(field);
        update.setCounterOp(counterOp);
        pbMapOp.updates.push(update);
    }
    var j, v, setOp;
    for (i = 0; i < mapOp.sets.adds.length; i++) {
        update = new MapUpdate();
        field = new MapField();
        field.setName(new Buffer(mapOp.sets.adds[i].key));
        field.setType(MapField.MapFieldType.SET);
        setOp = new SetOp();
        for (j = 0; j < mapOp.sets.adds[i].add.length; j++) {
            v = maybeConvertStringToBuffer(mapOp.sets.adds[i].add[j]);
            setOp.adds.push(v);
        }
        update.setField(field);
        update.setSetOp(setOp);
        pbMapOp.updates.push(update);
    }
    for (i = 0; i < mapOp.sets.removes.length; i++) {
        update = new MapUpdate();
        field = new MapField();
        field.setName(new Buffer(mapOp.sets.removes[i].key));
        field.setType(MapField.MapFieldType.SET);
        setOp = new SetOp();
        for (j = 0; j < mapOp.sets.removes[i].remove.length; j++) {
            v = maybeConvertStringToBuffer(mapOp.sets.removes[i].remove[j]);
            setOp.removes.push(v);
        }
        update.setField(field);
        update.setSetOp(setOp);
        pbMapOp.updates.push(update);
    }
    for (i = 0; i < mapOp.registers.set.length; i++) {
        update = new MapUpdate();
        field = new MapField();
        field.setName(new Buffer(mapOp.registers.set[i].key));
        field.setType(MapField.MapFieldType.REGISTER);
        update.setField(field);
        v = maybeConvertStringToBuffer(mapOp.registers.set[i].value);
        update.setRegisterOp(v);
        pbMapOp.updates.push(update);
    }
    for (i = 0; i < mapOp.flags.set.length; i++) {
        update = new MapUpdate();
        field = new MapField();
        field.setName(new Buffer(mapOp.flags.set[i].key));
        field.setType(MapField.MapFieldType.FLAG);
        update.setField(field);
        if (mapOp.flags.set[i].state) {
            update.setFlagOp(MapUpdate.FlagOp.ENABLE);
        } else {
            update.setFlagOp(MapUpdate.FlagOp.DISABLE);
        }
        pbMapOp.updates.push(update);
    }
    for (i = 0; i < mapOp.maps.modify.length; i++) {
        update = new MapUpdate();
        field = new MapField();
        field.setName(new Buffer(mapOp.maps.modify[i].key));
        field.setType(MapField.MapFieldType.MAP);
        var nestedMapOp = new MapOp();
        this._populate(mapOp.maps.modify[i].map, nestedMapOp);
        update.setMapOp(nestedMapOp);
        update.setField(field);
        pbMapOp.updates.push(update);
    }
};

var schema = Joi.object().keys({
    bucket: Joi.string().required(),
    bucketType: Joi.string().required(),
    key: Joi.binary().default(null).optional(),
    op: Joi.object().type(MapOperation).required(),
    w: Joi.number().default(null).optional(),
    dw: Joi.number().default(null).optional(),
    pw: Joi.number().default(null).optional(),
    returnBody: Joi.boolean().default(true).optional(),
    setsAsBuffers: Joi.boolean().default(false).optional(),
    timeout: Joi.number().default(null).optional(),
    context: Joi.binary().default(null).optional()
});

/**
 * A builder for constructing UpdateMap instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a UpdateMap directly, this builder may be used.
 *
 *     var update = new UpdateMap.Builder()
 *               .withBucketType('counters')
 *               .withBucket('myBucket')
 *               .withKey('counter_1')
 *               .withMapOperation(mapOp)
 *               .withCallback(callback)
 *               .build();
 *
 * See {{#crossLink "UpdateMap.MapOperation"}}UpdateMap.MapOperation{{/crossLink}}
 *
 * @class UpdateMap.Builder
 * @constructor
 */
function Builder() {}

Builder.prototype = {

    /**
     * Set the bucket.
     * @method withBucket
     * @param {String} bucket the bucket in Riak
     * @chainable
     */
    withBucket : function(bucket) {
        this.bucket = bucket;
        return this;
    },
    /**
     * Set the bucket type.
     * @method withBucketType
     * @param {String} bucketType the bucket type in riak
     * @chainable
     */
    withBucketType : function(bucketType) {
        this.bucketType = bucketType;
        return this;
    },
    /**
     * Set the key.
     * If not set, riak will generate and return a key.
     * @method withKey
     * @param {String} key the key in riak.
     * @chainable
     */
    withKey : function(key) {
        this.key = key;
        return this;
    },
    /**
     * Set the modifications to make to this map.
     * @method withMapOperation
     * @param {MapOperation} mapOp the modifications
     * @chainable
     */
    withMapOperation : function(mapOp) {
        this.op = mapOp;
        return this;
    },
    /**
     * The context returned from a previous fetch of this map.
     * __Note:__ this is required when performing any removes.
     * @method withContext
     * @param {Buffer} context the contect from a previous fetch.
     * @chainable
     */
    withContext : function(context) {
        this.context = context;
        return this;
    },
    /**
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback The callback to be executed when the operation completes.
     * @param {String} callback.err An error message. Will be null if no error.
     * @param {Object} callback.response The response from Riak. Will be null if returnBody is not set.
     * @param {String} callback.response.generatedKey If no key was supplied, Riak will generate and return one here.
     * @param {Buffer} callback.response.context An opaque context to be used in any subsequent modification of the map.
     * @param {Object} callback.response.map The map in Riak, converted to a JS object.
     * @chainable
     */
    withCallback : function(callback) {
        this.callback = callback;
        return this;
    },
     /**
    * Set the W value.
    * How many replicas to write to before returning a successful response.
    * If not set the bucket default is used.
    * @method withW
    * @param {number} w the W value.
    * @chainable
    */
    withW : function(w) {
        this.w = w ;
        return this;
    },
    /**
     * Set the DW value.
     * How many replicas to commit to durable storage before returning a successful response.
     * If not set the bucket default is used.
     * @method withDw
     * @param {number} dw the DW value.
     * @chainable
     */
    withDw : function(dw) {
        this.dw = dw;
        return this;
    },
    /**
     * Set the PW value.
     * How many primary nodes must be up when the write is attempted.
     * If not set the bucket default is used.
     * @method withPw
     * @param {number} pw the PW value.
     * @chainable
     */
    withPw : function(pw) {
        this.pw = pw;
        return this;
    },
    /**
    * Return the counter after updating.
    * @method withReturnBody
    * @param {boolean} returnBody true to return the counter.
    * @chainable
    */
    withReturnBody: function(returnBody) {
        this.returnBody = returnBody;
        return this;
    },
    /**
     * Return sets as arrays of Buffers rather than strings.
     * By default the contents of sets are converted to strings. Setting this
     * to true will cause this not to occur and the raw bytes returned
     * as Buffer objects. Note this is only used with the returnBody option.
     * @method withSetsAsBuffers
     * @param {Boolean} setsAsBuffers true to not convert set contents to strings.
     * @chainable
     */
    withSetsAsBuffers : function(setsAsBuffers) {
        this.setsAsBuffers = setsAsBuffers;
        return this;
    },
    /**
    * Set a timeout for this operation.
    * @method withTimeout
    * @param {number} timeout a timeout in milliseconds.
    * @chainable
    */
    withTimeout : function(timeout) {
        this.timeout = timeout;
        return this;
    },
    /**
     * Construct an UpdateMap instance.
     * @method build
     * @return {UpdateMap}
     */
    build : function() {
        var cb = this.callback;
        delete this.callback;
        return new UpdateMap(this, cb);
    }

};

/**
 * Class that encapsulates modifications to a Map in Riak.
 *
 * Rather than manually constructing this yourself, a fluent API is provided.
 *
 *     var mapOp = new UpdateMap.MapOperation();
 *     mapOp.incrementCounter('counter_1', 50)
 *         .addToSet('set_1', 'set_value_1')
 *         .setRegister('register_1', new Buffer('register_value_1'))
 *         .setFlag('flag_1', true)
 *         .map('inner_map')
 *             .incrementCounter('counter_1', 50)
 *             .addToSet('set_2', 'set_value_2');
 *
 * @class UpdateMap.MapOperation
 * @constructor
 */
function MapOperation() {
    this.counters = { increment: [], remove : []};
    this.maps = { modify: [], remove: []};
    this.sets = { adds: [], removes: [], remove: []};
    this.registers = { set: [], remove: [] };
    this.flags = { set: [], remove: [] };
}

MapOperation.prototype = {

    /**
     * Increment (and/or create) a counter inside the map.
     * @method incrementCounter
     * @param {String} key the key in the map for this counter.
     * @param {Number} increment the amount to increment (or decrement if negative)
     * @chainable
     */
    incrementCounter : function(key, increment) {
        this._removeRemove(this.counters.remove, key);
        var op = this._getOp(this.counters.increment, key);
        if (op) {
            op.increment += increment;
        } else {
            this.counters.increment.push({key: key, increment: increment});
        }
        return this;
    },
    /**
     * Remove a counter from a map.
     * @method removeCounter
     * @param {String} key the key in the map for this counter.
     * @chainable
     */
    removeCounter : function(key) {
        this._removeAddsOrRemoves(this.counters.increment, key);
        if (this.counters.remove.indexOf(key) === -1) {
            this.counters.remove.push(key);
        }
        return this;
    },
    /**
     * Add a value to a set (and create is necessary) in a map.
     * @method addToSet
     * @param {String} key the key for the set in the map.
     * @param {String|Buffer} value the value to add to the set.
     * @chainable
     */
    addToSet : function(key, value) {
        this._removeRemove(this.sets.remove, key);
        var op = this._getOp(this.sets.adds, key);
        if (op) {
            op.add.push(value);
        } else {
            this.sets.adds.push({key: key, add: [value]});
        }
        return this;
    },
    /**
     * Remove a value from a set in a map.
     * @method removeFromSet
     * @param {String} key the key for the set in the map.
     * @param {String|Buffer} value the value to remove from the set.
     * @chainable
     */
    removeFromSet : function(key, value) {
        this._removeRemove(this.sets.remove, key);
        var op = this._getOp(this.sets.removes, key);
        if (op) {
            op.remove.push(value);
        } else {
            this.sets.removes.push({key: key, remove: [value]});
        }
        return this;
    },
    /**
     * Remove a set from a map.
     * @method removeSet
     * @param {String} key the key for the set in the map.
     * @chainable
     */
    removeSet : function(key) {
        this._removeAddsOrRemoves(this.sets.adds, key);
        this._removeAddsOrRemoves(this.sets.removes, key);
        if (this.sets.remove.indexOf(key) === -1) {
            this.sets.remove.push(key);
        }
        return this;
    },
    /**
     * Set a register in a map.
     * @method setRegister
     * @param {String} key the key for the register in the map.
     * @param {String|Buffer} value the value for the register.
     * @chainable}
     */
    setRegister : function(key, value) {
        this._removeRemove(this.registers.remove, key);
        var op = this._getOp(this.registers.set, key);
        if (op) {
            op.value = value;
        } else {
            this.registers.set.push({key: key, value: value});
        }
        return this;
    },
    /**
     * Remove a register from a map.
     * @method removeRegister
     * @param {String} key the key for the register in the map.
     * @chainable
     */
    removeRegister : function(key) {
        this._removeAddsOrRemoves(this.registers.set, key);
        if (this.registers.remove.indexOf(key) === -1) {
            this.registers.remove.push(key);
        }
        return this;
    },
    /**
     * Set a flag in a map.
     * @method setFlag
     * @param {String} key the key for the set in the map.
     * @param {Boolean} value the value for the flag.
     * @chainable}
     */
    setFlag : function(key, state) {
        this._removeRemove(this.flags.remove, key);
        var op = this._getOp(this.flags.set, key);
        if (op) {
            op.state = state;
        } else {
            this.flags.set.push({key: key, state: state});
        }
        return this;
    },
    /**
     * Remove a flag from a map.
     * @method removeFlag
     * @param {String} key the key for the flag in the map.
     * @chainable
     */
    removeFlag : function(key) {
        this._removeAddsOrRemoves(this.flags.set, key);
        if (this.flags.remove.indexOf(key) === -1) {
            this.flags.remove.push(key);
        }
        return this;
    },
    /**
     * Access/create a map inside the current one.
     * This adds/accesses a nested map and returns a reference to another
     * MapOperation that applies to it. You can then modify the components of
     * that map;
     *
     *     mapOp.map('inner_map')
     *         .incrementCounter('counter_1', 50)
     *         .addToSet('set_2', 'set_value_2');
     * @method map
     * @param {String} key the key for the nested map in the current map.
     * @chainable
     */
    map : function(key) {
        this._removeRemove(this.maps.remove, key);
        var map = this._getOp(this.maps.modify);
        if (map) {
            return map;
        } else {
            map = new MapOperation();
            this.maps.modify.push({key: key, map: map});
            return map;
        }
    },
    /**
     * Remove a map from a map.
     * @method removeMap
     * @param {String} key the key for the map in the map.
     * @chainable
     */
    removeMap : function(key) {
        this._removeAddsOrRemoves(this.maps.modify, key);
        if (this.maps.remove.indexOf(key) === -1) {
            this.maps.remove.push(key);
        }
        return this;
    },
    _removeRemove : function(array, key) {
        for (var i = 0; i < array.length; i++) {
            if (array[i] === key) {
                array.splice(i,1);
            }
        }
    },
    _removeAddsOrRemoves : function(array, key) {
        for (var i = 0; i < array.length; i++) {
            if (array[i].key === key) {
                array.splice(i,1);
            }
        }
    },
    _getOp : function(array, key) {
        for (var i = 0; i < array.length; i++) {
            if (array[i].key === key) {
                return array[i];
            }
        }
        return null;
    },
    _hasRemoves : function() {

        var nestedHaveRemoves = false;
        for (var i = 0; i < this.maps.modify.length; i++) {
            nestedHaveRemoves |= this.maps.modify[i].map._hasRemoves();
        }

        return nestedHaveRemoves ||
                this.counters.remove.length ||
                this.maps.remove.length ||
                this.sets.remove.length ||
                this.sets.removes.length ||
                this.registers.remove.length ||
                this.flags.remove.length;
    }

};

module.exports = UpdateMap;
module.exports.MapOperation = MapOperation;
module.exports.Builder = Builder;

