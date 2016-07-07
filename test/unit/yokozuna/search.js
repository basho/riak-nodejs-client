'use strict';

var Search = require('../../../lib/commands/yokozuna/search');
var RpbPair = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbPair');
var RpbSearchDoc = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbSearchDoc');
var RpbErrorResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbErrorResp');
var RpbSearchQueryResp = require('../../../lib/protobuf/riakprotobuf').getProtoFor('RpbSearchQueryResp');
var assert = require('assert');

describe('Search', function() {
    describe('Build', function() {
        it('builds-RpbSearchQueryReq', function(done) {
            var search = new Search.Builder()
                    .withIndexName('indexName')
                    .withQuery('some solr query')
                    .withNumRows(20)
                    .withStart(10)
                    .withSortField('someField')
                    .withFilterQuery('someQuery')
                    .withDefaultField('defaultField')
                    .withDefaultOperation('and')
                    .withReturnFields(['field1', 'field2'])
                    .withPresort('key')
                    .withCallback(function(){})
                    .build();

            var protobuf = search.constructPbRequest();

            assert.equal(protobuf.index.toString('utf8'), 'indexName');
            assert.equal(protobuf.q.toString('utf8'), 'some solr query');
            assert.equal(protobuf.rows, 20);
            assert.equal(protobuf.start, 10);
            assert.equal(protobuf.sort.toString('utf8'), 'someField');
            assert.equal(protobuf.filter.toString('utf8'), 'someQuery');
            assert.equal(protobuf.df.toString('utf8'), 'defaultField');
            assert.equal(protobuf.op.toString('utf8'), 'and');
            assert.equal(protobuf.fl.length, 2);
            assert.equal(protobuf.fl[0].toString('utf8'), 'field1');
            assert.equal(protobuf.fl[1].toString('utf8'), 'field2');
            assert.equal(protobuf.presort.toString('utf8'), 'key');
            done();
        });

        it('processes-RpbSearchQueryResp', function(done) {
            var resp = new RpbSearchQueryResp();

            var doc = new RpbSearchDoc();
            // The PB API is broken in that it returns everything as strings. The
            // search command should convert boolean and numeric values properly
            var pair = new RpbPair();
            pair.key = new Buffer('leader_b');
            pair.value = new Buffer('true');
            doc.fields.push(pair);

            pair = new RpbPair();
            pair.key = new Buffer('leader2_b');
            pair.value = new Buffer('false');
            doc.fields.push(pair);

            pair = new RpbPair();
            pair.key = new Buffer('age_i');
            pair.value = new Buffer('30');
            doc.fields.push(pair);

            pair = new RpbPair();
            pair.key = new Buffer('age_f');
            pair.value = new Buffer('12.34');
            doc.fields.push(pair);

            pair = new RpbPair();
            pair.key = new Buffer('_yz_id');
            pair.value = new Buffer('default_cats_liono_37');
            doc.fields.push(pair);

            pair = new RpbPair();
            pair.key = new Buffer('nullValue');
            pair.value = new Buffer('null');
            doc.fields.push(pair);

            pair = new RpbPair();
            pair.key = new Buffer('emptyString');
            pair.value = new Buffer('');
            doc.fields.push(pair);

            resp.docs.push(doc);
            resp.max_score = 1.123;
            resp.num_found = 1;

            var callback = function(err, response) {
                assert.equal(response.numFound, 1);
                assert.equal(response.maxScore, 1.123);
                assert.equal(response.docs.length, 1);
                var doc = response.docs[0];
                assert.strictEqual(doc.leader_b, true);
                assert.strictEqual(doc.leader2_b, false);
                assert.strictEqual(doc.age_i, 30);
                assert.strictEqual(doc.age_f, 12.34);
                assert.strictEqual(doc.nullValue, null);
                assert.strictEqual(doc.emptyString, '');
                assert.strictEqual(doc._yz_id, 'default_cats_liono_37');
                done();
            };

            var search = new Search.Builder()
                    .withIndexName('indexName')
                    .withQuery('some solr query')
                    .withCallback(callback)
                    .build();
            search.onSuccess(resp);
        });

        it('processes-RpbSearchQueryResp-gh-165', function(done) {
            /*
                {
                "state": "connected",
                "hardwareId": 2,
                "_yz_id": "1*ssh-sessions*ssh-sessions*4e05ed89-2a49-4f17-8885-ca79f0d292c0*41",
                "_yz_rk": "4e05ed89-2a49-4f17-8885-ca79f0d292c0",
                "_yz_rt": "ssh-sessions",
                "_yz_rb": "ssh-sessions"
                }
            */
            var resp = new RpbSearchQueryResp();
            var doc = new RpbSearchDoc();
            var pair = new RpbPair();
            pair.key = new Buffer('state');
            pair.value = new Buffer('connected');
            doc.fields.push(pair);

            pair = new RpbPair();
            pair.key = new Buffer('hardwareId');
            pair.value = new Buffer('2');
            doc.fields.push(pair);

            pair = new RpbPair();
            pair.key = new Buffer('_yz_id');
            pair.value = new Buffer('1*ssh-sessions*ssh-sessions*4e05ed89-2a49-4f17-8885-ca79f0d292c0*41');
            doc.fields.push(pair);

            pair = new RpbPair();
            pair.key = new Buffer('_yz_rk');
            pair.value = new Buffer('4e05ed89-2a49-4f17-8885-ca79f0d292c0');
            doc.fields.push(pair);

            pair = new RpbPair();
            pair.key = new Buffer('_yz_rt');
            pair.value = new Buffer('ssh-sessions');
            doc.fields.push(pair);

            pair = new RpbPair();
            pair.key = new Buffer('_yz_rb');
            pair.value = new Buffer('ssh-sessions');
            doc.fields.push(pair);

            resp.docs.push(doc);
            resp.max_score = 1.123;
            resp.num_found = 1;

            var callback = function(err, response) {
                assert.equal(response.numFound, 1);
                assert.equal(response.maxScore, 1.123);
                assert.equal(response.docs.length, 1);
                var doc = response.docs[0];
                assert.strictEqual(doc.state, 'connected');
                assert.strictEqual(doc.hardwareId, '2');
                assert.strictEqual(doc._yz_id, '1*ssh-sessions*ssh-sessions*4e05ed89-2a49-4f17-8885-ca79f0d292c0*41');
                assert.strictEqual(doc._yz_rk, '4e05ed89-2a49-4f17-8885-ca79f0d292c0');
                assert.strictEqual(doc._yz_rt, 'ssh-sessions');
                assert.strictEqual(doc._yz_rb, 'ssh-sessions');
                done();
            };

            var search = new Search.Builder()
                    .withIndexName('indexName')
                    .withQuery('some solr query')
                    .withConvertDocuments(false)
                    .withCallback(callback)
                    .build();
            search.onSuccess(resp);
        });

        it ('processes-RpbErrorResp', function(done) {
           var rpbErrorResp = new RpbErrorResp();
           rpbErrorResp.setErrmsg(new Buffer('this is an error'));

           var callback = function(err, response) {
               if (err) {
                   assert.equal(err,'this is an error');
                   done();
               }
           };

           var search = new Search.Builder()
                    .withIndexName('indexName')
                    .withQuery('some solr query')
                    .withCallback(callback)
                    .build();
           search.onRiakError(rpbErrorResp);
       });
    });
});
