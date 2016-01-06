'use strict';

var utils = require('../../utils');
var rpb = require('../../protobuf/riakprotobuf');
var RpbContent = rpb.getProtoFor('RpbContent');
var RpbPair = rpb.getProtoFor('RpbPair');
var RpbLink = rpb.getProtoFor('RpbLink');

/**
 * Provides the RiakObject class.
 * @module KV
 */

/**
 * A class that encapsulates the metadata and value stored in Riak.
 * 
 * While you can fetch and store regular JS objects with {{#crossLink "FetchValue"}}{{/crossLink}}
 * and {{#crossLink "StoreValue"}}{{/crossLink}}, if you want access to the associated 
 * metadata stored in Riak with the value you'll want to use a RiakObject instead. 
 *
 * @class RiakObject
 * @constructor
 */
function RiakObject() {
    // default content type
    this.contentType = 'application/json';
}

RiakObject.prototype = {
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
     * 
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
     * Set the value.
     * @method setValue
     * @param {String|Buffer|Object} value the value stored in Riak.
     * @chainable
     */
    setValue : function(value) {
        this.value = value;
        return this;
    },
    /**
     * Get the value.
     * 
     * This will either be a Buffer or a plain JS object.
     * @method getValue
     * @return {Buffer|Object} The value returned from Riak.
     */
    getValue : function() {
        return this.value;
    },
    /**
     * Set the content type.
     *  
     * Due to Riak's HTTP API this is represented as a string suitable for
     * a HTTP Content-Type header. 
     * 
     * If not set, the default is 'application/json'
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
     * 
     * Due to Riak's HTTP API this is represented as a string suitable for
     * a HTTP Content-Type header. 
     * @method getContentType
     * @return {String} the content type.
     */
    getContentType : function() {
        return this.contentType;
    },
    /**
     * Set the content encoding.
     *
     * @method setContentEncoding
     * @param {String} contentEncoding the content encoding
     * @chainable
     */
    setContentEncoding : function(contentEncoding) {
        this.contentEncoding = contentEncoding;
        return this;
    },
    /**
     * Get the content encoding
     *
     * @method getContentEncoding
     * @returns {String} the content encoding
     */
    getContentEncoding : function() {
        return this.contentEncoding;
    },
    /**
     * Set the user meta data.
     * 
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
     * Determine if any user metadata is present.
     * @method hasUserMeta
     * @return {Boolean} true if user meta data is present.
     */
    hasUserMeta : function() {
        return this.userMeta !== undefined;
    },
    /**
     * Get the user meta data
     * 
     * This is an array of key/value objects
     * @method getUserMeta
     * @return {Object[]} array of key/value objects
     */
    getUserMeta : function() {
        return this.userMeta;
    },
    /**
     * Set an index, replacing the existing value if any.
     * @method setIndex
     * @param {String} indexName the index name
     * @param {String[]|Number[]} arrayOfKeys - the keys 
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
     * 
     * If the index does not exist it will be created.
     * @method addToIndex
     * @param {String} indexName the index name
     * @param {String|Number} ...key 1 or more keys to add
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
     * Determine if any links are present.
     * 
     * Note that link walking is a deprecated feature in Riak 2.0.
     * 
     * See: [Link Walking](http://docs.basho.com/riak/latest/dev/using/link-walking/)
     * @method hasLinks
     * @return {Boolean} true if there are any links.
     * @deprecated Link walking is a deprecated feature in Riak 2.0.
     */
    hasLinks : function() {
        return this.links !== undefined;
    },
    /**
     * Get the links.
     * 
     * This is an array of objects representing links to other objects in Riak.
     * 
     * Note that link walking is a deprecated feature in Riak 2.0.
     * 
     * See: [Link Walking](http://docs.basho.com/riak/latest/dev/using/link-walking/)
     * @method getLinks
     * @return {Object[]} An array containing the links, or undefined if none exist.
     * @deprecated Link walking is a deprecated feature in Riak 2.0.
     */
    getLinks : function() {
        return this.links;
    },
    /**
     * Set the links.
     * 
     * This is an array of objects representing links to other objects in Riak.
     * 
     * Note that link walking is a deprecated feature in Riak 2.0.
     * 
     * See: [Link Walking](http://docs.basho.com/riak/latest/dev/using/link-walking/)
     * @method setLinks
     * @param {Object[]} links An array of objects representing the links.
     * @param {String} links.bucket The bucket the linked object is in.
     * @param {String} links.key The key for the linked object.
     * @param {String} links.tag The identifier that describes the relationship you are wishing to capture with your link
     * @chainable
     * @deprecated Link walking is a deprecated feature in Riak 2.0.
     */
    setLinks : function(links) {
        this.links = links;
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
     * @method getIsTombstone
     * @return {Boolean} true if this is a tombstone.
     */
    getIsTombstone : function() {
        return this.isTombstone;
    },
    /**
     * Returns the last modified time of this RiakObject.
     * 
     * The timestamp is returned as a (Unix) epoch time. 
     * @method getLastModified
     * @return {Number} the last modified time. 
     */
    getLastModified : function() {
        return this.lastModified;
    }
};

module.exports = RiakObject;

module.exports.isRiakObject = function (v) {
    return v instanceof RiakObject;
};

module.exports.isIntIndex = function(indexName) {
    return indexName.indexOf('_int', indexName.length - 4) !== -1;
};

module.exports.createFromRpbContent = function(rpbContent, convertToJs) {
    var ro = new RiakObject();
    
    var value = rpbContent.getValue();
    
    ro.isTombstone = rpbContent.getDeleted() ? true : false;

    if (ro.isTombstone) {
        ro.value = {};
    } else {
        // ReturnHead will only retun metadata
        if (convertToJs && value) {
            ro.value = JSON.parse(value.toString('utf8')); 
        } else if (value) {
            ro.value = value.toBuffer();
        }
    }
    
    if (rpbContent.getContentType()) {
        ro.contentType = rpbContent.getContentType().toString('utf8');
    }

    if (rpbContent.getContentEncoding()) {
        ro.contentEncoding = rpbContent.getContentEncoding().toString('utf8');
    }
    
    if (rpbContent.getLastMod()) {
        var lm = rpbContent.getLastMod();
        var lmu = rpbContent.getLastModUsecs();
        ro.lastModified = ((lm * 1000) + (lmu / 1000));
    }
    
    // UserMeta
    var pbUsermeta = rpbContent.getUsermeta();
    var usermeta = new Array(pbUsermeta.length);
    var i;
    for (i = 0; i < pbUsermeta.length; i++) {
        usermeta[i] = { key: pbUsermeta[i].key.toString('utf8'), 
            value: pbUsermeta[i].value.toString('utf8') };
    }
    ro.userMeta = usermeta;
    
    //indexes
    var pbIndexes = rpbContent.getIndexes();
    if (pbIndexes.length > 0) {
        var indexes = {};
        for (i = 0; i < pbIndexes.length; i++) {
            var indexName = pbIndexes[i].key.toString('utf8');
            if (!indexes.hasOwnProperty(indexName)) {
                indexes[indexName] = [];
            }
            var stringValue = pbIndexes[i].value.toString('utf8');
            if (RiakObject.isIntIndex(indexName)) {
                indexes[indexName].push(parseInt(stringValue));
            } else {
                indexes[indexName].push(stringValue);
            }
        }
        ro.indexes = indexes;
    }
    
    //links
    var pbLinks = rpbContent.getLinks();
    if (pbLinks.length) {
        var links = new Array(pbLinks.length);
        var link;
        for (i = 0; i < pbLinks.length; i++) {
            link = {};
            if (pbLinks[i].bucket) {
                link.bucket = pbLinks[i].bucket.toString('utf8');
            }
            if (pbLinks[i].key) {
                link.key = pbLinks[i].key.toString('utf8');
            }
            if (pbLinks[i].tag) {
                link.tag = pbLinks[i].tag.toString('utf8');
            }
            links[i] = link;
        }
        ro.links = links;
    }
    
    return ro;
};

/**
 * Creates and returns a RpbContent protobuf from a value and meta.
 * 
 * If the value is a JS Object it is converted using JSON.stringify().
 * @private
 * @method populateRpbContentFromRiakObject
 * @static
 * @param {RiakObject} ro The RiakObject
 * @return {Object} a RpbContent protobuf
 */
module.exports.populateRpbContentFromRiakObject = function(ro) {
    var rpbContent = new RpbContent();
    
    if (utils.isString(ro.value)) {
        rpbContent.setValue(new Buffer(ro.value));
    } else if (Buffer.isBuffer(ro.value)) {
        rpbContent.setValue(ro.value);
    } else {
        rpbContent.setValue(new Buffer(JSON.stringify(ro.value)));
    }
    
    if (ro.getContentType()) {
        rpbContent.setContentType(new Buffer(ro.getContentType()));
    }

    if (ro.getContentEncoding()) {
        rpbContent.setContentEncoding(new Buffer(ro.getContentEncoding()));
    }

    var i, pair;
    if (ro.hasIndexes()) {
        var allIndexes = ro.getIndexes();
        for (var indexName in allIndexes) {
            var indexKeys = allIndexes[indexName];
            for (i = 0; i < indexKeys.length; i++) {
                pair = new RpbPair();
                pair.setKey(new Buffer(indexName));
                // The Riak API expects string values, even for _int indexes
                pair.setValue(new Buffer(indexKeys[i].toString()));
                rpbContent.indexes.push(pair);
            }
        }
    }

    if (ro.hasUserMeta()) {
        var userMeta = ro.getUserMeta();
        for (i = 0; i < userMeta.length; i++) {
            pair = new RpbPair();
            pair.setKey(new Buffer(userMeta[i].key));
            pair.setValue(new Buffer(userMeta[i].value));
            rpbContent.usermeta.push(pair);
        }
    }
    
    if (ro.hasLinks()) {
        var links = ro.getLinks();
        var pbLink;
        for (i = 0; i < links.length; i++) {
            pbLink = new RpbLink();
            if (links[i].bucket) {
                pbLink.setBucket(new Buffer(links[i].bucket));
            }
            if (links[i].key) {
                pbLink.setKey(new Buffer(links[i].key));
            }
            if (links[i].tag) {
                pbLink.setTag(new Buffer(links[i].tag));
            }
            rpbContent.links.push(pbLink);
        }
    }
    return rpbContent;
};

