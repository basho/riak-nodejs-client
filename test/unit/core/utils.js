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

var assert = require('assert');
var logger = require('winston');

var utils = require('../../../lib/core/utils');

var state = Object.freeze({
    CREATED : 0,
    RUNNING : 1,
    HEALTH_CHECKING : 2,
    SHUTTING_DOWN : 3,
    SHUTDOWN : 4
});

var stateNames = Object.freeze({
    0 : 'CREATED',
    1 : 'RUNNING',
    2 : 'HEALTH_CHECKING',
    3 : 'SHUTTING_DOWN',
    4 : 'SHUTDOWN'
});

describe('core-utils', function() {
    it('state-check-failure', function(done) {
        assert.throws(
            function () {
                utils.stateCheck('FRAZZLE',
                    state.RUNNING, [state.CREATED, state.SHUTDOWN], stateNames);
            },
            function (err) {
                logger.debug('[t/u/c/utils] saw err:', err.message);
                return err instanceof Error &&
                    /RUNNING/.test(err) &&
                    /CREATED/.test(err) &&
                    /SHUTDOWN/.test(err);
            },
            'unexpected error!'
        );
        done();
    });
});
