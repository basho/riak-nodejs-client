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

/**
 * Base class for KV responses from Riak
 * @class KvResponseBase
 * @constructor
 * @param {KvValueMetaPair[]} valueMetaPairs an array of KvValueMetaPairs from Riak.
 */
function KvResponseBase(valueMetaPairs) {

    this.values = valueMetaPairs;
    this._defaultConflictResolver = function(values) {
        if (values.length > 1) {
            throw "Siblings present and no conflict resolver supplied.";
        } else {
            return values[0];
        }
    };

}

/**
 * Determine if this response contains any returned values.
 * @method hasValues
 * @return {Boolean} true if values are present, false otherwise.
 */
KvResponseBase.prototype.hasValues = function() {
    return this.values.length !== 0;
};

/**
* Return the number of values contained in this response.
* If siblings are present the response will contain more than one value.
* @method getNumberOfValues
* @return {Number} the number of values in this response.
*/
KvResponseBase.prototype.getNumberOfValues = function() {
    return this.values.length;
};

/**
* Get the vector clock returned with this response.
* @method getVClock
* @return {Buffer} The vector clock or undefined if one is not present.
*/
KvResponseBase.prototype.getVClock = function() {
    if (this.hasValues()) {
        return values[0].meta.getVClock();
    } else {
        return undefined;
    }
};

/**
* Get all the objects returned in this response.
* 
* Returns an array containing KvValueMetaPairs with the value
* as a Buffer.
*     
* If siblings were present in Riak for the key you were fetching, 
* this method will return all of them to you.
* @method getValues
* @return {KvValueMetaPair[]} a list of objects containing the value as a Buffer and the meta
*/
KvResponseBase.prototype.getValues = function() {
    return this.values;
};

/**
 * Get a single, resolved value and meta from this resposne.
 * 
 * The __conflictResolver__ will be passed the array of KvValueMetaPairs
 * with the value as a Buffer.
 *
 * If a conflict resolver is not supplied and siblings are present, an
 * exception will be thrown. Otherwise a single KvValueMetaPair is returned.
 * 
 * @method getValue
 * @param {Function} [conflictResolver] function to resolve siblings
 * @param {KvValueMetaPair[]} conflictResolver.values an array of KvValueMetaPair objects
 * @return {KvValueMetaPair|Object} the return value of the conflictResolver. 
 */
KvResponseBase.prototype.getValue = function(conflictResolver) {
    if (!conflictResolver) {
        conflictResolver = this._defaultConflictResolver;
    }
    return conflictResolver(this.values);
};

/**
* Get all the objects returned in this response.
* 
* Returns an array containing KvValueMetaPairs. 
*     
* The value portion is converted to a JS object via JSON.parse().
* 
* If siblings were present in Riak for the key you were fetching, 
* this method will return all of them to you.
* @method getValues
* @return {KvValueMetaPair[]} an array of KvValueMetaPairs
*/
KvResponseBase.prototype.getValuesAsJs = function() {
    var newArray = new Array(this.values.length);
    for (var i = 0; i < this.values.length; i++) {
        newArray[i] = new KvValueMetaPair(JSON.parse(this.values[i].value.toString('utf8')),
            this.values[i].meta);
    }
    return newArray;
};

/**
 * Get a single, resolved value and meta from this resposne.
 * 
 * The __conflictResolver__ will be passed the array of KvValueMetaPairs
 * returned by Riak. 
 * 
 * The value is converted by passing the Buffer returned from Riak to
 * JSON.parse().    
 * 
 * If a conflict resolver is not supplied and siblings are present, an
 * exception will be thrown. Otherwise a single KvValueMetaPair is returned.
 * 
 * @method getValue
 * @param {Function} [conflictResolver] function to resolve siblings
 * @param {KvValueMetaPair[]} conflictResolver.values an array of KvValueMetaPairs
 * @return {KvValueMetaPair|Object} the return value of the conflictResolver. 
 */
KvResponseBase.prototype.getValueAsJs = function(conflictResolver) {
    if (!conflictResolver) {
        conflictResolver = this._defaultConflictResolver;
    }
    return conflictResolver(this.getValuesAsJs());
};

/**
 * @namespace KvResponseBase
 * @class KvValueMetaPair
 * @constructor 
 * @param {Buffer|Object} value the value retrieved from Riak, either as a Buffer or an Object converted via JSON.parse()
 * @param {RiakMeta} meta the metadata associated with the value in Riak.
 */
function KvValueMetaPair(value, meta) {
    /**
     * The value returned from Riak
     * @property value
     * @type Buffer|Object A Buffer or Object resulting from passing the Buffer to JSON.parse()
     */
    this.value = value;
    /**
     * The metadata associated with the value in Riak
     * @property meta
     * @type RiakMeta 
     */
    this.meta = meta;
};

/**
 * Get the value. 
 * @return {Buffer|Object} The value retrieved from Riak, either as a Buffer or an Object converted via JSON.parse()
 */
KvValueMetaPair.prototype.getValue = function() {
    return this.value;
};

/**
 * Get the metadata.
 * @return {RiakMeta} the metadata associated with the value in Riak.
 */
KvValueMetaPair.prototype.getMeta = function() {
    return this.meta;
};

module.exports = KvResponseBase;
module.exports.KvValueMetaPair = KvValueMetaPair;
