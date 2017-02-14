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

var CommandBase = require('../commandbase');
var errors = require('../../errors');
var tsutils = require('./utils');

var inherits = require('util').inherits;
var Joi = require('joi');

/**
 * Provides the ListKeys class, its builder, and its response.
 * @module TS
 */

/**
 * Command used to list keys in a table.
 *
 * As a convenience, a builder class is provided;
 *
 *     var listKeys = new ListKeys.Builder()
 *                  .withTable('myTable')
 *                  .withCallback(myCallback)
 *                  .build();
 *
 * See {{#crossLink "ListKeys.Builder"}}ListKeys.Builder{{/crossLink}}
 * @class ListKeys
 * @constructor
 * @param {Object} options The options for this command
 * @param {Boolean} [options.allowListing=false] Whether to allow this command. Must be set to true or exception will result.
 * @param {String} options.table The table in Riak TS.
 * @param {Boolean} [options.stream=true] Whether to stream or accumulate the result before calling callback.
 * @param {Number} [options.timeout] Set a timeout for this operation.
 * @param {Function} callback The callback to be executed when the operation completes.
 * @param {String} callback.err An error message. Will be null if no error.
 * @param {Object} callback.response the keys returned from Riak.
 * @param {Boolean} callback.response.done True if you have received all the keys.
 * @param {String[]} callback.response.keys The array of keys.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function ListKeys(options, callback) {
    CommandBase.call(this, 'TsListKeysReq', 'TsListKeysResp', callback);
    this.validateOptions(options, schema);
    if (!this.options.stream) {
        this.keys = [];
    }
    if (!this.options.allowListing) {
        throw errors.ListError();
    }
}

inherits(ListKeys, CommandBase);

ListKeys.prototype.constructPbRequest = function () {
    var protobuf = this.getPbReqBuilder();
    protobuf.setTable(new Buffer(this.options.table));
    protobuf.setTimeout(this.options.timeout);
    return protobuf;
};

ListKeys.prototype.onSuccess = function (rpbTsListKeysResp) {
    var keysToSend = new Array(rpbTsListKeysResp.keys.length);
    if (rpbTsListKeysResp.keys.length) {
        for (var i = 0; i < rpbTsListKeysResp.keys.length; i++) {
            var tsrow = rpbTsListKeysResp.keys[i];
            var keycells = tsutils.convertTsRow(tsrow);
            keysToSend[i] = keycells;
        }
    }

    if (this.options.stream) {
        this._callback(null, {
            table: this.options.table, keys: keysToSend, done: rpbTsListKeysResp.done
        });
    } else {
        Array.prototype.push.apply(this.keys, keysToSend);
        if (rpbTsListKeysResp.done) {
            this._callback(null, {
                table: this.options.table, keys: this.keys, done: rpbTsListKeysResp.done
            });
        }
    }

    return rpbTsListKeysResp.done;
};

var schema = Joi.object().keys({
   allowListing : Joi.boolean().default(false),
   table: Joi.string().required(),
   stream : Joi.boolean().default(true),
   timeout: Joi.number().default(null)
});

/**
 * A builder for constructing ListKeys instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a ListKeys directly, this builder may be used.
 *
 *     var listKeys = new ListKeys.Builder()
 *                  .withTable('table')
 *                  .withCallback(myCallback)
 *                  .build();
 *
 * @class ListKeys.Builder
 * @constructor
 */
function Builder() {}

Builder.prototype = {
    /**
     * Allow listing.
     * @method withAllowListing
     * @chainable
     */
    withAllowListing : function() {
        this.allowListing = true;
        return this;
    },
    /**
     * Set the table.
     * @method withTable
     * @param {String} table The table in Riak TS
     * @chainable
     */
    withTable : function (table) {
        this.table = table;
        return this;
    },
    /**
     * Stream the results.
     * Setting this to true will cause you callback to be called as the results
     * are returned from Riak TS. Set to false the result set will be buffered and
     * delevered via a single call to your callback. Note that on large result sets
     * this is very memory intensive.
     * @method withStreaming
     * @param {Boolean} [stream=true] Set whether or not to stream the results
     * @chainable
     */
    withStreaming : function (stream) {
        this.stream = stream;
        return this;
    },
    /**
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback The callback to be executed when the operation completes.
     * @param {String} callback.err An error message. Will be null if no error.
     * @param {Object} callback.response the keys returned from Riak.
     * @param {Boolean} callback.response.done True if you have received all the keys.
     * @param {String} callback.response.table The table the keys are from.
     * @param {String[]} callback.response.keys The array of keys.
     * @chainable
     */
    withCallback : function (callback) {
        this.callback = callback;
        return this;
    },
    /**
    * Set a timeout for this operation.
    * @method withTimeout
    * @param {Number} timeout a timeout in milliseconds.
    * @chainable
    */
    withTimeout : function (timeout) {
        this.timeout = timeout;
        return this;
    },
    /**
     * Construct a ListKeys instance.
     * @method build
     * @return {ListKeys}
     */
    build : function () {
        var cb = this.callback;
        delete this.callback;
        return new ListKeys(this, cb);
    }
};

module.exports = ListKeys;
module.exports.Builder = Builder;
