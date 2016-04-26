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
