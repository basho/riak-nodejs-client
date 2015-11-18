'use strict';

var logger = require('winston');
logger.remove(logger.transports.Console);
logger.add(logger.transports.Console, {
    level : 'debug',
    colorize: true,
    timestamp: true
});
