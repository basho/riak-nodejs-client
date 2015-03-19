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

var RpbContent = require('../../protobuf/riakprotobuf').getProtoFor('RpbContent');
var RpbPair = require('../../protobuf/riakprotobuf').getProtoFor('RpbPair');

/**
 * Provides the RiakMeta class.
 * @module RiakMeta
 */

/**
 * A class that encapsulates the metadata attached to an value in Riak.
 * @class RiakMeta
 * @constructor
 */
function RiakMeta() {

}

RiakMeta.prototype = {
    
    /**
     * Set the key.
     * @method setKey
     * @param {String} key the key in Riak.
     * @chainable
     */
    setKey : function(key) {
        this.key = key;
        return this;
    },
    /**
     * Get the key.
     * @method getKey
     * @return {String} the key.
     */
    getKey : function() {
        return this.key;
    },
    /**
     * Set the bucket.
     * @method setBucket
     * @param {String} bucket the bucket in Riak.
     * @chainable
     */
    setBucket : function(bucket) {
        this.bucket = bucket;
        return this;
    },
    /**
     * Get the bucket.
     * @method getBucket
     * @return {String} the bucket. 
     */
    getBucket : function() {
        return this.bucket;
    },
    /**
     * Set the bucket type.
     * If this is not set 'default' is used.
     * @method setBucketType
     * @param {String} bucketType the bucket type in Riak.
     * @chainable
     */
    setBucketType : function(bucketType) {
        this.bucketType = bucketType;
        return this;
    },
    /**
     * Get the bucket type.
     * @method getBucketType
     * @return {String} the bucket type.
     */
    getBucketType : function() {
      return this.bucketType;  
    },
    /**
     * Set the content type. 
     * Due to Riak's HTTP API this is represented as a string suitable for
     * a HTTP Content-Type header. If not set, the default is 'application/json'
     * @method setContentType
     * @param {String} contentType the content type.
     * @chainable
     */
    setContentType : function(contentType) {
        this.contentType = contentType;
        return this;
    },
    /**
     * Get the content type.
     * Due to Riak's HTTP API this is represented as a string suitable for
     * a HTTP Content-Type header. 
     * @method getContentType
     * @return {String} the content type.
     */
    getContentType : function() {
        return this.contentType;
    },
    /**
     * Set the user meta data.
     * This is an array of key/value objects.
     * @method setUserMeta
     * @param {Object[]} userMeta
     * @param {String} userMeta.key usermeta key
     * @param {String} userMeta.value usermeat value
     * @chainable
     */
    setUserMeta : function(userMeta) {
        this.userMeta = userMeta;
        return this;
    },
    /**
     * Determine if any user meta dta is present.
     * @method hasUserMeta
     * @return {Boolean} true if user meta data is present.
     */
    hasUserMeta : function() {
        return this.userMeta !== undefined;
    },
    /**
     * Get the user meta data
     * This is an array of key/value objects
     * @method getUserMeta.
     * @return {Object[]} array of key/value objects
     */
    getUserMeta : function() {
        return this.userMeta;
    },
    /**
     * Set an index, replacing the existing value is any.
     * @method setIndex
     * @param {String} indexName the index name
     * @param {String[]} arrayOfKeys - the keys 
     * @chainable
     */
    setIndex : function(indexName, arrayOfKeys) {
        if (this.indexes === undefined) {
            this.indexes = {};
        }
        this.indexes[indexName] = arrayOfKeys;
        return this; 
    },
    /**
     * Determine if any indexes are present.
     * @method hasIndexes
     * @return {Boolean} true if indexes are present.
     */
    hasIndexes : function() {
        return this.indexes !== undefined;
    },
    /**
     * Get all indexes.
     *
     * @method getIndexes
     * @return {Object} an object whose fields are the index names holding arrays of keys
     */
    getIndexes : function() {
        return this.indexes;
    },
    /**
     * Get the keys for an index.
     * @method getIndex
     * @param {String} indexName the name of the index
     * @return {String[]} the keys 
     */
    getIndex : function(indexName) {
        if (this.indexes !== undefined) {
            return this.indexes[indexName];
        } else {
            return undefined;
        }
    },
    /**
     * Add one or more keys to an index.
     * If the index does not exist it will be created.
     * @method addToIndex
     * @param {String} indexName the index name
     * @param {String} ...key 1 or more keys to add
     * @chainable
     */
    addToIndex : function(indexName, key) {
        if (this.indexes === undefined) {
            this.indexes = {};
        }
        if (!this.indexes.hasOwnProperty(indexName)) {
            this.indexes[indexName] = [];
        }
        for (var i = 1; i < arguments.length; i++) {
            this.indexes[indexName].push(arguments[i]);
        }
        return this;
    },
    /**
     * Set the vector clock.
     * @method setVClock
     * @param {Buffer} vclock the vclock retrieved from Riak
     * @chainable
     */
    setVClock : function(vclock) {
        this.vclock = vclock;
        return this;
    },
    /**
     * Get the vector clock.
     * @method getVClock
     * @return {Buffer} The vector clock retrieved from Riak.
     */
    getVClock : function() {
        return this.vclock;
    },
    /**
     * Returns whether or not this RiakObject is marked as being deleted (a tombstone)
     * @return {Boolean} true if this is a tombstone.
     */
    isTombstone : function() {
        return this.isTombstone;
    },
    /**
     * Returns the last modified time of this RiakObject.
     * The timestamp is returned as a (Unix) epoch time. 
     * @method getLastModified
     * @return {Number} the last modified time. 
     */
    getLastModified : function() {
        return this.lastModified;
    }
    
    
    
};


module.exports = RiakMeta;

module.exports.extractMetaFromRpbContent = function(rpbContent, vclock, bucketType, bucket, key ) {

    var meta = new RiakMeta();
    
    if (rpbContent.getContentType()) {
        meta.contentType = rpbContent.getContentType().toString('utf8');
    }
    
    if (rpbContent.getLastMod()) {
        var lm = rpbContent.getLastMod();
        var lmu = rpbContent.getLastModUsecs();
        meta.lastModified = ((lm * 1000) + (lmu / 1000));
    }

    
    meta.isTombstone = rpbContent.getDeleted() ? true : false;
    
    // UserMeta
    var pbUsermeta = rpbContent.getUsermeta();
    var usermeta = new Array(pbUsermeta.length);
    for (var i = 0; i < pbUsermeta.length; i++) {
        usermeta[i] = { key: pbUsermeta[i].key.toString('utf8'), 
            value: pbUsermeta[i].value.toString('utf8') };
    }
    meta.userMeta = usermeta;
    
    
    //indexes
    var pbIndexes = rpbContent.getIndexes();
    if (pbIndexes.length > 0) {
        var indexes = {};
        for (var i =0; i < pbIndexes.length; i++) {
            var indexName = pbIndexes[i].key.toString('utf8');
            if (!indexes.hasOwnProperty(indexName)) {
                indexes[indexName] = [];
            }
            indexes[indexName].push(pbIndexes[i].value.toString('utf8'));
        }
        meta.indexes = indexes;
    }
    
    //TODO: links
    
    meta.vclock = vclock;
    meta.bucket = bucket;
    meta.bucketType = bucketType;
    meta.key = key;
    
    return meta;
    
};

/**
 * Creates and returns a RpbContent protobuf from a value and meta.
 * 
 * If the value is a JS Object it is converted using JSON.stringify().
 * 
 * @param {String|Buffer|Object} value the value
 * @param {RiakMeta} [meta] The metadata 
 * @return {Object} a RpbContent protobuf
 */
module.exports.populateRpbContentFromValueAndMeta = function(value, meta) {
    var rpbContent = new RpbContent();
    
    if (typeof value === 'string') {
        rpbContent.setValue(new Buffer(value));
    } else if (value instanceof Buffer) {
        rpbContent.setValue(value);
    } else {
        rpbContent.setValue(new Buffer(JSON.stringify(value)));
    }
    
    if (meta) {
    
        if (meta.getContentType()) {
            rpbContent.setContentType(new Buffer(meta.getContentType()));
        }

        if (meta.hasIndexes()) {
            var allIndexes = meta.getIndexes();
            for (var indexName in allIndexes) {
                var indexKeys = allIndexes[indexName];
                for (var i = 0; i < indexKeys.length; i++) {
                    var pair = new RpbPair();
                    pair.setKey(new Buffer(indexName));
                    pair.setValue(new Buffer(indexKeys[i]));
                    rpbContent.indexes.push(pair);
                }
            }
        }

        if (meta.hasUserMeta()) {
            var userMeta = meta.getUserMeta();
            for (var i = 0; i < userMeta.length; i++) {
                var pair = new RpbPair();
                pair.setKey(new Buffer(userMeta[i].key));
                pair.setValue(new Buffer(userMeta[i].value));
                rpbContent.usermeta.push(pair);
            }
        }
    }
    
    return rpbContent;
        
};


