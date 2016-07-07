'use strict';

var CommandBase = require('../commandbase');
var inherits = require('util').inherits;
var Joi = require('joi');

var utils = require('../../utils');

/**
 * Provides the (Yokozuna) Search class, its builder, and its response.
 * @module YZ
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
 * See {{#crossLink "Search.Builder"}}Search.Builder{{/crossLink}}
 *
 * For more information on Riak Search (Yokozuna/Solr) see:
 * [Using Search](http://docs.basho.com/riak/latest/dev/using/search/)
 *
 * @class Search
 * @constructor
 * @param {Object} options The options for this command.
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
 * @param {Boolean} [options.convertDocuments] Convert Solr document values to JS types.
 * @param {Function} callback The callback to execute when the comman completes.
 * @param {String} callback.err an error message. Will be null if no error.
 * @param {Object} callback.response the response from Riak (Solr)
 * @param {Number} callback.response.numFound The number of documents found.
 * @param {Number} callback.response.maxScore The max score value.
 * @param {Object[]} callback.response.docs Array of documents returned from Solr.
 * @param {Object} callback.data additional error data. Will be null if no error.
 * @extends CommandBase
 */
function Search(options, callback)  {
    CommandBase.call(this, 'RpbSearchQueryReq', 'RpbSearchQueryResp', callback);
    this.validateOptions(options, schema);
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

// https://github.com/basho/yokozuna/blob/develop/include/yokozuna.hrl#L340-L404
function isYzField(f) {
    return f === '_yz_id'   ||
           f === '_yz_ed'   ||
           f === '_yz_fpn'  ||
           f === '_yz_vtag' ||
           f === '_yz_pn'   ||
           f === '_yz_rk'   ||
           f === '_yz_rt'   ||
           f === '_yz_rb'   ||
           f === '_yz_err';
}

Search.prototype.onSuccess = function(rpbSearchQueryResp) {

    var docsToReturn = new Array(rpbSearchQueryResp.docs.length);
    for (var i = 0; i < rpbSearchQueryResp.docs.length; i++) {
        var doc = {};
        for (var j = 0; j < rpbSearchQueryResp.docs[i].fields.length; j++) {
            var key = rpbSearchQueryResp.docs[i].fields[j].key.toString('utf8');
            var value = rpbSearchQueryResp.docs[i].fields[j].value.toString('utf8');
            // GH-165 do not convert well-known YZ fields
            if (isYzField(key) === false && this.options.convertDocuments === true) {
                // Search and MapReduce are effectively broken with the PB API because
                // everything is returned as a string.
                var valAsNum = utils.maybeIsNumber(value);
                if (valAsNum) {
                    // it's a Long or number.
                    value = valAsNum;
                } else if (value === 'null') {
                    // It's actually null
                    value = null;
                } else {
                    // might also be a boolean.
                    value = value === 'true' || (value === 'false' ? false : value);
                }
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
    convertDocuments: Joi.boolean().default(true).optional()
});

/**
 * A builder for constructing Search instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a Search directly, this builder may be used.
 *
 *     var search = new Search.Builder()
 *                    .withIndexName(myIndex)
 *                    .withQuery(myQuery)
 *                    .withNumRows(10)
 *                    .withCallback(myCallback)
 *                    .build();
 *
 * @class Search.Builder
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
    * Convert values in documents returned by Solr to corresponding Javascript types.
    * @method convertDocuments
    * @param {Boolean} convert Solr document values to JS types.
    * @chainable
    */
   withConvertDocuments : function(convertDocuments) {
       this.convertDocuments = convertDocuments;
       return this;
   },
   /**
     * Set the callback to be executed when the operation completes.
     * @method withCallback
     * @param {Function} callback The callback to execute when the comman completes.
     * @param {String} callback.err an error message. Will be null if no error.
     * @param {Object} callback.response the response from Riak (Solr)
     * @param {Number} callback.response.numFound The number of documents found.
     * @param {Number} callback.response.maxScore The max score value.
     * @param {Object[]} callback.response.docs Array of documents returned from Solr.
     * @chainable
     */
   withCallback : function(callback) {
       this.callback = callback;
       return this;
   },
   /**
    * Construct a new Search instance.
    * @method build
    * @return {Search}
    */
   build : function() {
        var cb = this.callback;
        delete this.callback;
        return new Search(this, cb);
   }

};

module.exports = Search;
module.exports.Builder = Builder;
