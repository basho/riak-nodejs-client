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

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');


/**
 * Provides the (Yokozuna) Search class, its builder, and its response.
 * @module Search
 */

/**
 * Command used to perform a (Yokozuna) search.
 * 
 * As a convenience, a builder class is provided:
 * 
 *      var search = new Search.Builder()
 *                      .withIndexName(myIndex)
 *                      .withQuery(myQuery)
 *                      .withNumRows(10)
 *                      .withCallback(myCallback)
 *                      .build();
 *      
 * 
 * @class Search
 * @constructor
 * @param {Object} options the options for this command.
 * @param {String} options.indexName Set the index name used for this search.
 * @param {String} options.q Set the Solr query string.
 * @param {Number} [options.maxRows=10] Specify the maximum number of results to return.
 * @param {String} [options.start=0] Specify the starting result of the query.
 * @param {String} [options.sortField] Sort the results on the specified field name.
 * @param {String} [options.filterQuery] Filters the search by an additional query scoped to inline fields.
 * @param {String} [options.defaultField] Use the provided field as the default. Overrides the “default_field” setting in the schema file.
 * @param {String} [options.defaultOperation] Set the default operation. Allowed settings are either “and” or “or”.
 * @param {String[]} [options.returnFields] Only return the provided fields.
 * @param {String} [options.presort] Sorts all of the results. Either "key" or "score".
 * @param {Function} callback the callback to execute when the comman completes.
 * @param {String} callback.err an error message
 * @param {Object} response the response from Riak (Solr)
 * @extends CommandBase
 */
function Search(options, callback)  {
    CommandBase.call(this, 'RpbSearchQueryReq', 'RpbSearchQueryResp', callback);
    
    var self = this;
    Joi.validate(options, schema, function(err, options) {
       
        if (err) {
            throw err;
        }
    
        self.options = options;
    });
    
    this.remainingTries = 1;
}

inherits(Search, CommandBase);

Search.prototype.constructPbRequest = function() {
  
    var protobuf = this.getPbReqBuilder();
    protobuf.index = new Buffer(this.options.indexName);
    protobuf.q = new Buffer(this.options.q);
    protobuf.rows = this.options.maxRows;
    protobuf.start = this.options.start;
    if (this.options.sortField) {
        protobuf.sort = new Buffer(this.options.sortField);
    }
    if (this.options.filterQuery) {
        protobuf.filter = new Buffer(this.options.filterQuery);
    }
    if (this.options.defaultField) {
        protobuf.df = new Buffer(this.options.defaultField);
    }
    if (this.options.defaultOperation) {
        protobuf.op = new Buffer(this.options.defaultOperation);
    }
    if (this.options.presort) {
        protobuf.presort = new Buffer(this.options.presort);
    }
    for (var i = 0; i < this.options.returnFields.length; i++) {
        protobuf.fl.push(new Buffer(this.options.returnFields[i]));
    }
    
    return protobuf;
    
};

Search.prototype.onSuccess = function(rpbSearchQueryResp) {

    var docsToReturn = new Array(rpbSearchQueryResp.docs.length);
    for (var i = 0; i < rpbSearchQueryResp.docs.length; i++) {
        var doc = {};
        for (var j = 0; j < rpbSearchQueryResp.docs[i].fields.length; j++) {
            var key = rpbSearchQueryResp.docs[i].fields[j].key.toString('utf8');
            var value = rpbSearchQueryResp.docs[i].fields[j].value.toString('utf8');
            // Search and MapReduce are effectively broken with the PB API because 
            // everything is returned as a string.
            if (!isNaN(value)) {
                // it's really a number. 
                value = +value;
            } else if (value === 'null') {
                // It's actually null
                value = null;
            } else {
                // might also be a boolean. 
                value = value === 'true' || (value === 'false' ? false : value);
            } 
            //Support multiple values for the same key
            if (doc.hasOwnProperty(key)) {
                if (doc[key].constructor === Array) {
                    doc[key].push(value);
                } else {
                    doc[key] = [ doc[key], value ];
                }
            } else {
                doc[key] = value;
            }
        }
        docsToReturn[i] = doc;
    }
    var result = { numFound : rpbSearchQueryResp.num_found,
                   maxScore : rpbSearchQueryResp.max_score,
                   docs: docsToReturn };
               
    this._callback(null, result);
    return true;

};

var schema = Joi.object().keys({
    indexName: Joi.string().required(),
    q: Joi.string().required(),
    maxRows: Joi.number().default(null).optional(),
    start: Joi.number().default(null).optional(),
    sortField: Joi.string().default(null).optional(),
    filterQuery: Joi.string().default(null).optional(),
    defaultField: Joi.string().default(null).optional(),
    defaultOperation: Joi.string().default(null).optional(),
    returnFields: Joi.array().default([]).optional(),
    presort: Joi.string().default(null).optional(),
    callback: Joi.func().strip().optional()
});

/**
 * A builder for constructing Search instances.
 * * Rather than having to manually construct the __options__ and instantiating
 * a Search directly, this builder may be used.
 * 
 *      var search = new Search.Builder()
 *                      .withIndexName(myIndex)
 *                      .withQuery(myQuery)
 *                      .withNumRows(10)
 *                      .withCallback(myCallback)
 *                      .build();
 *       
 * @namespace Search
 * @class Builder
 * @constructor
 */
function Builder() {}

Builder.prototype = {
  
    /**
     * Set the index name used for this search.
     * @method withIndexName
     * @param {String} indexName the name of the yokozuna index
     * @chainable
     */
    withIndexName : function(indexName) {
        this.indexName = indexName;
        return this;
    },
    /**
     * Set the Solr query string.
     * All distributed Solr queries are supported, which actually 
     * includes most of the single-node Solr queries. 
     * @method withQuery
     * @param {String} queryString the query
     * @chainable
     */
    withQuery : function(queryString) {
        this.q = queryString;
        return this;
    },
    /**
    * Specify the maximum number of results to return.
    * Riak defaults to 10 if this is not set.
    * @method withNumRows
    * @param {Number} maxRows the maximum number of results to return.
    * @chainable
    */
    withNumRows : function(maxRows) {
        this.maxRows = maxRows;
        return this;
    },
    /**
    * Specify the starting result of the query.
    * Useful for pagination. The default is 0.
    * @method withStart
    * @param {Number} start the index of the starting result.
    * @chainable
    */
   withStart : function(start) {
       this.start = start;
       return this;
   },
   /**
    * Sort the results on the specified field name.
    * Default is “none”, which causes the results to be sorted in descending order by score.
    * @method withSortField
    * @param {String} sortField the fieldname to sort the results on.
    * @chainable
    */
   withSortField : function(sortField) {
       this.sortField = sortField;
       return this;
   },
   /**
    * Filters the search by an additional query scoped to inline fields.
    * @method withFilterQuery
    * @param {String} filterQuery the filter query.
    * @chainable
    */
   withFilterQuery : function(filterQuery) {
       this.filterQuery = filterQuery;
       return this;
   },
   /**
    * Use the provided field as the default.
    * Overrides the “default_field” setting in the schema file.
    * @method withDefaultField
    * @param {String} fieldName the name of the field.
    * @chainable
    */
   withDefaultField : function(fieldName) {
       this.defaultField = fieldName;
       return this;
   },
   /**
    * Set the default operation.
    * Allowed settings are either “and” or “or”.
    * Overrides the “default_op” setting in the schema file.
    * @method withDefaultOperation
    * @param {String} op A string containing either "and" or "or".
    * @chainable
    */
   withDefaultOperation : function(op) {
       this.defaultOperation = op;
       return this;
   },
   /**
    * Only return the provided fields.
    * Filters the results to only contain the provided fields.
    * @method withReturnFields
    * @param {String[]} fields an array of field names.
    * @chainable
    */
   withReturnFields : function(fields) {
       this.returnFields = fields;
       return this;
   },
   /**
    * Sorts all of the results by bucket key, or the search score, before the given rows are chosen.
    * This is useful when paginating to ensure the results are returned in a consistent order.
    * @method withPresort
    * @param {String} presort a string containing either "key" or "score".
    * @chainable
    */
   withPresort : function(presort) {
       this.presort = presort;
       return this;
   },
   /**
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback - the callback to execute
     * @param {String} callback.err An error message
     * @param {Object} callback.response the response from Yokozuna (Solr)
     * @chainable
     */
   withCallback : function(callback) {
       this.callback = callback;
       return this;
   },
   /**
    * Construct a new Search instance.
    * @return {Search}
    */
   build : function() {
       return new Search(this, this.callback);
   }
    
};

module.exports = Search;
module.exports.Builder = Builder;
