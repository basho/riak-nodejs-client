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
 * @param {Object[]} rawResponse and array of value/meta pairs from Riak.
 * @param {Buffer} rawResponse.value The value portion
 * @param {RiakMeta} rawResponse.meta The metadata
 */
function KvResponseBase() {

    this.values = arguments;
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
* Returns an array containing value/meta pairs.
*     [ { value: Buffer, meta: RiakMeta }, ... ]  
* If siblings were present in Riak for the key you were fetching, 
* this method will return all of them to you.
* @method getValues
* @return {Object[]} a list of objects containing the value and the meta
*/
KvResponseBase.prototype.getValues = function() {
    return values;
};

/**
 * Get a single, resolved value and meta from this resposne.
 * 
 * The __conflictResolver__ will be passed the array of value/meta 
 * pairs returned by Riak. 
 *     [ { value: Buffer, meta: RiakMeta }, ... ] 
 *
 * If a conflict resolver is not supplied and siblings are present, an
 * exception will be thrown.
 * 
 * @method getValue
 * @param {Function} [conflictResolver] function to resolve siblings
 * @param {Object[]} conflictResolver.values an array of value/meta pairs
 * @return {Object} the return value of the conflictResolver. 
 */
KvResponseBase.prototype.getValue = function(conflictResolver) {
    if (!conflictResolver) {
        conflictResolver = this._defaultConflictResolver;
    }
    callback(conflictResolver(this.values));
    return conflictResolver(this.values);
};

/**
* Get all the objects returned in this response.
* 
* Returns an array containing value/meta pairs.
*     [ { value: Object, meta: RiakMeta }, ... ]  
* The value Object is created by passing the value returned from Riak to
* JSON.parse().    
* If siblings were present in Riak for the key you were fetching, 
* this method will return all of them to you.
* @method getValues
* @return {Object[]} a list of objects containing the value and the meta
*/
KvResponseBase.prototype.getValuesAsJs = function() {
    var newArray = new Array(this.values.length);
    for (var i = 0; i < this.values.length; i++) {
        newArray[i] = { value: JSON.parse(this.values[i].value.toString('utf8')),
            meta: this.values[i].meta };
    }
    return newArray;
};

/**
 * Get a single, resolved value and meta from this resposne.
 * 
 * The __conflictResolver__ will be passed the array of value/meta 
 * pairs returned by Riak. 
 *     [ { value: Object, meta: RiakMeta }, ... ] 
 * The value Object is created by passing the value returned from Riak to
 * JSON.parse().    
 * 
 * If a conflict resolver is not supplied and siblings are present, an
 * exception will be thrown.
 * 
 * @method getValue
 * @param {Function} [conflictResolver] function to resolve siblings
 * @param {Object[]} conflictResolver.values an array of value/meta pairs
 * @return {Object} the return value of the conflictResolver. 
 */
KvResponseBase.prototype.getValueAsJs = function(conflictResolver) {
    if (!conflictResolver) {
        conflictResolver = this._defaultConflictResolver;
    }
    return conflictResolver(this.getValuesAsJs());
};


module.exports = KvResponseBase;
