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
 * @param {RiakObject[]} riakobjects an array of RiakObjects from Riak.
 */
function KvResponseBase(riakobjects) {

    this.riakObjects = riakobjects;
}

/**
 * Get the objects returned from Riak.
 * If siblings were present and no conflict resolver was provided all
 * siblings are returned. 
 * @method getRiakObjects
 * @returns {RiakObject[]} an array containing the returned objects.
 */
KvResponseBase.prototype.getRiakObjects = function() {
    return this.riakObjects;
};

module.exports = KvResponseBase;
