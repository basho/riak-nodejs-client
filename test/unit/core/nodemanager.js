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

var assert = require('assert');
var logging = require('winston');
var joi = require('joi');

var LeastExecutingNodeManager = require('../../../lib/core/leastexecutingnodemanager.js');

function getManager(shuffle) {
    var m = new LeastExecutingNodeManager(shuffle);
    m.tryExecute = function (node, command) {
        logging.debug('[LeastExecutingNodeManager-TEST] executing %s on node (%d:%d)',
            command.name, node.id, node.executeCount);
        node.executed = true;
        return true;
    };
    return m;
}

describe('NodeManager', function() {
    describe('LeastExecutingNodeManager', function() {
        it('sorts by executeCount to choose a node', function(done) {
            var n = [
                { executeCount: 3, id: 0 }, 
                { executeCount: 2, id: 1 }, 
                { executeCount: 1, id: 2 } 
            ];
            var m = getManager(false);
            assert(m.executeOnNode(n, { name: 'TEST COMMAND' }));
            assert(n[2].executed);
            assert.equal(n[2].id, 2);
            done();
        });

        it('sorts by executeCount and shuffles to choose a node', function(done) {
            var n = [
                { executeCount: 3, id: 0 }, 
                { executeCount: 2, id: 1 }, 
                { executeCount: 1, id: 2 },
                { executeCount: 1, id: 3 }, 
                { executeCount: 1, id: 4 } 
            ];
            var m = getManager(true);

            for (var k = 0; k < n.length * 10; k++) {
                assert(m.executeOnNode(n, { name: 'TEST COMMAND' }));
                var found = false;
                for (var i = 2; i <= 4; i++) {
                    if (n[i].executed) {
                        assert(n[i].id === 2 || n[i].id === 3 || n[i].id === 4);
                        found = true;
                        break;
                    }
                }
                assert(found);
            }

            done();
        });
    });
});
