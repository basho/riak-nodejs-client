/**
 *
 * Copyright 2014-present Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

var UpdateSetBase = require('./updatesetbase');
var inherits = require('util').inherits;
var Joi = require('joi');
var ByteBuffer = require('bytebuffer');

var utils = require('../../utils');
var rpb = require('../../protobuf/riakprotobuf');
var DtOp = rpb.getProtoFor('DtOp');
var GSetOp = rpb.getProtoFor('GSetOp');

/**
 * Provides the Update GSet class, its builder, and its response.
 * @module CRDT
 */

/**
 * Command used tp update a gset in Riak
 *
 * As a convenience, a builder class is provided:
 *
 *        var update = new UpdateGSet.Builder()
 *               .withBucketType('gsets')
 *               .withBucket('myBucket')
 *               .withKey('set_1')
 *               .withAdditions(['this', 'that', 'other'])
 *               .withCallback(callback)
 *               .build();
 *
 * See {{#crossLink "UpdateSet.Builder"}}UpdateSet.Builder{{/crossLink}}
 * @class UpdateGSet
 * @constructor
 * @param {String[]|Buffer[]} [options.additions] The values to be added to the set.
 * @extends UpdateSetBase
 */
function UpdateGSet(options, callback) {
    var gset_opts = {
        additions: options.additions
    };
    delete options.additions;

    UpdateSetBase.call(this, options, callback);

    var self = this;
    Joi.validate(gset_opts, schema, function(err, opts) {
        if (err) {
            throw err;
        }
        self.additions = opts.additions;
    });
}

inherits(UpdateGSet, UpdateSetBase);

UpdateGSet.prototype.constructDtOp = function() {
    var dt_op = new DtOp();
    var gset_op = new GSetOp();
    dt_op.setGsetOp(gset_op);
    gset_op.adds = UpdateSetBase.buflist(this.additions);
    return dt_op;
};

UpdateGSet.prototype.getUpdateRespValues = function(dtUpdateResp) {
    return dtUpdateResp.gset_value;
};

var schema = Joi.object().keys({
    additions: Joi.array().default([]).optional(),
});

/**
 * A builder for constructing UpdateGSet instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a UpdateGSet directly, this builder may be used.
 *
 *     var update = new UpdateGSet.Builder()
 *                       .withBucketType('myBucketType')
 *                       .withBucket('myBucket')
 *                       .withKey('myKey')
 *                       .withAdditions(['this', 'that', 'other'])
 *                       .withCallback(myCallback)
 *                       .build();
 *
 * @class UpdateGSet.Builder
 * @extends UpdateSetBase.Builder
 */
function Builder() {
    UpdateSetBase.Builder.call(this);
}

inherits(Builder, UpdateSetBase.Builder);

/**
 * Construct an UpdateGSet instance.
 * @method build
 * @return {UpdateGSet}
 */
Builder.prototype.build = function() {
    var cb = this.callback;
    delete this.callback;
    return new UpdateGSet(this, cb);
};

/**
 * The values you wish to add to this gset.
 * @method withAdditions
 * @param {String[]|Buffer[]} additions The values to add.
 * @chainable
 */
utils.bb(Builder, 'additions');

module.exports = UpdateGSet;
module.exports.Builder = Builder;
