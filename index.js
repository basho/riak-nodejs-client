'use strict';

if (process.env.RIAK_NODEJS_CLIENT_DEBUG) {
    var logger = require('winston');
    logger.remove(logger.transports.Console);
    logger.add(logger.transports.Console, {
        level : 'debug',
        colorize: true,
        timestamp: true
    });
}

module.exports = require('./lib/client');
