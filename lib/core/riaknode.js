'use strict';

var async = require('async');
var backoff = require('backoff');
var events = require('events');
var Joi = require('joi');
var logger = require('winston');
var util = require('util');

var RiakConnection = require('./riakconnection');
var Ping = require('../commands/ping');
var utils = require('./utils');

var rpb = require('../protobuf/riakprotobuf');
var rpbErrorRespCode = rpb.getCodeFor('RpbErrorResp');

var nid = 0;

/**
 * @module Core
 */

/**
 * Provides the RiakNode class and its Builder.
 *
 * Instances of this class maintain connections to and execute commands on
 * a Riak node in a Riak cluster.
 *
 * __options__ is an object with the following defaults:
 *
 *     {
 *       remoteAdddress: '127.0.0.1',
 *       remotePort: 8087,
 *       maxConnections: 128,
 *       minConnections: 1,
 *       idleTimeout: 10000,
 *       connectionTimeout: 3000,
 *       requestTimeout: 5000,
 *       cork: true
 *     }
 *
 * As a convenience a builder class is provided;
 *
 *     var newNode = new RiakNode.Builder().withMinConnections(10).build();
 *
 * See {{#crossLink "RiakNode.Builder"}}RiakNode.Builder{{/crossLink}}
*
 * @class RiakNode
 * @constructor
 * @param {Object} options The options for this RiakNode.
 * @param {String} [options.remoteAddress=127.0.0.1] The address for this node. Can also be a FQDN.
 * @param {Number} [options.remotePort=8087] The port to connect to.
 * @param {Number} [options.minConnections=1] Set the minimum number of active connections to maintain.
 * @param {Number} [options.maxConnections=128] Set the maximum number of connections allowed.
 * @param {Number} [options.idleTimeout=10000] Set the idle timeout used to reap inactive connections.
 * @param {Number} [options.connectionTimeout=3000] Set the connection timeout used when making new connections.
 * @param {Number} [options.requestTimeout=5000] Set the timeout used when executing commands.
 * @param {Object} [options.auth] Set the authentication information for connections made by this node.
 * @param {Boolean} [options.cork] Use 'cork' on all sockets. Default is true.
 * @param {Boolean} [options.externalLoadBalancer] This RiakNode object connects to a load balancer. Default is false.
 * @param {String} options.auth.user Riak username.
 * @param {String} [options.auth.password] Riak password. Not required if using user cert.
 * @param {String|Buffer} [options.auth.pfx] A string or buffer holding the PFX or PKCS12 encoded private key, certificate and CA certificates.
 * @param {String|Buffer} [options.auth.key] A string holding the PEM encoded private key.
 * @param {String} [options.auth.passphrase]  A string of passphrase for the private key or pfx.
 * @param {String|Buffer} [options.auth.cert]  A string holding the PEM encoded certificate.
 * @param {String|String[]|Buffer[]} [options.auth.ca] Either a string or list of strings of PEM encoded CA certificates to trust.
 * @param {String|String[]|Buffer[]} [options.auth.crl] Either a string or list of strings of PEM encoded CRLs (Certificate Revocation List).
 * @param {Boolean} [options.auth.rejectUnauthorized] A boolean indicating whether a server should automatically reject clients with invalid certificates. Only applies to servers with requestCert enabled.
 *
 */
function RiakNode(options) {
    events.EventEmitter.call(this);

    var self = this;

    if (options === undefined) {
        options = {};
    }

    Joi.validate(options, schema, function (err, options) {
        if (err) {
            throw err;
        }
        self.remoteAddress = options.remoteAddress;
        self.remotePort = options.remotePort;
        self.minConnections = options.minConnections;
        self.maxConnections = options.maxConnections;
        self.idleTimeout = options.idleTimeout;
        self.connectionTimeout = options.connectionTimeout;
        self.requestTimeout = options.requestTimeout;
        self.state = State.CREATED;
        self.auth = options.auth;
        self.cork = options.cork;
        self.externalLoadBalancer = options.externalLoadBalancer;
        self.healthCheck = options.healthCheck;
    });

    this.executeCount = 0;

    // This is to facilitate debugging
    this.name = util.format('[RiakNode] (%s:%d-%d)',
        this.remoteAddress, this.remotePort, nid);
    nid++;

    // Note: useful for debugging event issues
    // this.setMaxListeners(1);

    // private data
    var currentNumConnections = 0;
    var available = [];

    // protected funcs
    this._getConnectionFromPool = function (cmd_name) {
        var conn = null;
        do {
            if (available.length === 0) {
                break;
            }
            conn = available.shift();
            if (conn.closed) {
                /*
                 * NB: this is expected as connection callbacks can close connections
                 * while they remain within the connection pool
                 */
                logger.debug('%s execute of %s attempted on closed connection', this.name, cmd_name);
                conn.executeDone();
                conn = null;
            }
        } while (!conn);
        return conn;
    };

    this._returnConnectionToPool = function (conn) {
        if (this.state < State.SHUTTING_DOWN) {
            conn.executeDone();
            available.unshift(conn);
            logger.debug("%s _returnConnectionToPool a: %d, cnc: %d",
                this.name, available.length, currentNumConnections);
        } else {
            logger.debug('%s connection returned to pool during shutdown.', this.name);
            this._decrementConnectionCount();
            conn.close();
        }
    };

    this._decrementConnectionCount = function () {
        if ((currentNumConnections - 1) < 0) {
            logger.debug("%s cnc will decrement less than zero! (%d)",
                this.name, currentNumConnections);
        }
        currentNumConnections--;
    };

    this._executeAllowed = function () {
        return this.state === State.RUNNING;
    };

    this._createNewConnectionAllowed = function () {
        return this.state === State.RUNNING &&
               currentNumConnections < this.maxConnections;
    };

    function expireIdleConnections(name, minConnections, idleTimeout) {
        logger.debug("%s expiring idle connections", name);
        var now = Date.now();
        var count = 0;
        var na = [];
        var conn = null;
        while (available.length) {
            conn = available.shift();
            // NB: don't keep any closed connections around.
            if (conn.closed) {
                conn.executeDone();
                conn = null;
                continue;
            }

            // NB: if a connection is executing, keep it around
            if (conn.inFlight) {
                na.push(conn);
                continue;
            }

            // NB: don't expire past minConnections
            if (currentNumConnections <= minConnections) {
                na.push(conn);
                continue;
            }

            if ((now - conn.lastUsed) >= idleTimeout) {
                /* jshint validthis:true */
                this._decrementConnectionCount();
                conn.close();
                count++;
                continue;
            }

            na.push(conn);
        }
        available = na;
        logger.debug("%s expired %d connections.", name, count);
    }

    var expireTimer = null;
    this._startIdleExpiration = function () {
        var cb = expireIdleConnections.bind(this, this.name, this.minConnections, this.idleTimeout);
        expireTimer = setInterval(cb, 5000);
    };

    this._stopIdleExpiration = function () {
        clearInterval(expireTimer);
    };

    this._createNewConnection = function (postConnectFunc, postFailFunc, healthCheck) {
        currentNumConnections++;

        var conn = new RiakConnection({
            remoteAddress : this.remoteAddress,
            remotePort : this.remotePort,
            connectionTimeout : this.connectionTimeout,
            requestTimeout : this.requestTimeout,
            auth: this.auth,
            healthCheck: healthCheck,
            cork: this.cork
        });

        var self = this;

        conn.on('connected', function (conn) {
            logger.debug("%s conn.on-connected", this.name);
            conn.on('responseReceived', self._responseReceived.bind(self));
            conn.on('connectionClosed', self._connectionClosed.bind(self));
            postConnectFunc(conn);
        });

        conn.on('connectFailed', function (conn, err){
            // NB: when connectFailed is raised, conn is already closed
            logger.debug("%s conn.on-connectFailed", this.name);
            self._decrementConnectionCount();
            postFailFunc(err);
        });

        conn.connect();
    };

    this._makeNewConnectionFunc = function (node) {
        var f = function (async_cb) {
            var postConnect = function (conn) {
                node._returnConnectionToPool(conn);
                async_cb(null, true);
            };
            var postFail = function (err) {
                async_cb(err, false);
            };
            node._createNewConnection(postConnect, postFail);
        };
        return f;
    };

    var hcb = backoff.fibonacci({
        initialDelay: 10,
        maxDelay: 5000
    });
    function initHealthChecker(self) {
        logger.debug("%s initializing health checker", self.name);
        var hcFunc = self.healthCheck;

        function hcSuccess(newConn) {
            hcb.reset();
            self._returnConnectionToPool(newConn);
            self.state = State.RUNNING;
            logger.debug("%s health check success", self.name);
            self.emit(EVT_SC, self, self.state);
        }

        function hcFailed(err) {
            logger.debug("%s failed health check:", err);
            // NB: healthcheck interval *should* be less than re-try interval
            hcb.backoff();
        }

        function hcReady(n, d) {
            self._createNewConnection(hcSuccess, hcFailed, hcFunc);
        }

        function hcBackoffHandler(n, d) {
            logger.debug("%s running health check %d, delay: %d", self.name, n, d);
        }

        hcb.on('ready', hcReady);
        hcb.on('backoff', hcBackoffHandler);
    }
    initHealthChecker(this);

    this._doHealthCheck = function () {
        switch (this.state) {
            case State.HEALTH_CHECKING:
                if (this.externalLoadBalancer) {
                    logger.error('%s already in health check, external load balancer:',
                        this.name, this.externalLoadBalancer);
                } else {
                    logger.debug("%s already in health check", this.name);
                }
                break;
            case State.RUNNING:
                if (this.externalLoadBalancer) {
                    logger.warn('%s would have health checked, external load balancer:',
                        this.name, this.externalLoadBalancer);
                } else {
                    this.state = State.HEALTH_CHECKING;
                    this.emit(EVT_SC, this, this.state);
                    hcb.backoff();
                }
                break;
            default:
                logger.warn('%s health check attempted in state:',
                    this.name, stateNames[this.state]);
        }
    };

    var sdb = backoff.fibonacci({
        initialDelay: 125,
        maxDelay: 1000
    });
    sdb.failAfter(10);
    var sdbHandlersInitialized = false;
    function runShutdown(self, callback) {
        var conns = [];
        function closeConnections() {
            logger.debug("%s closing %d connections",
                self.name, currentNumConnections);
            var conn = null;
            while (available.length) {
                conn = available.shift();
                if (conn.closed) {
                    conn.executeDone();
                    conn = null;
                } else {
                    self._decrementConnectionCount();
                    conn.close();
                    conns.push(conn);
                }
            }
            logger.debug("%s closed connections (cnc: %d)",
                self.name, currentNumConnections);
        }

        function areConnsClosed() {
            closeConnections();
            var conns_closed = true;
            for (var i = 0; i < conns.length; i++) {
                var c = conns[i];
                if (!c.closed) {
                    conns_closed = false;
                    break;
                }
            }
            if (currentNumConnections > 0) {
                conns_closed = false;
            }
            return conns_closed;
        }

        function nodeShutdown() {
            sdb.reset();
            self.state = State.SHUTDOWN;
            logger.debug("%s shut down.", self.name);
            if (self.executeCount !== 0) {
                logger.warn('%s execution count (%d) NOT ZERO at shutdown',
                    self.name, self.executeCount);
            }
            if (currentNumConnections !== 0) {
                logger.warn('%s connection count (%d) NOT ZERO at shutdown',
                    self.name, currentNumConnections);
            }
            self.emit(EVT_SC, self, self.state);
            self.removeAllListeners();
            if (callback) {
                callback(null, self.state);
            }
        }

        function sdReady() {
            if (areConnsClosed()) {
                nodeShutdown();
            } else {
                logger.debug("%s connections still not closed", self.name);
                sdb.backoff();
            }
        }

        function sdBackoffHandler(n, d) {
            logger.debug("%s running shutdown %d, delay: %d", self.name, n, d);
        }

        function sdFailHandler() {
            logger.warn("%s shutting down after max tries", self.name);
            nodeShutdown();
        }

        if (!sdbHandlersInitialized) {
            sdb.on('ready', sdReady);
            sdb.on('backoff', sdBackoffHandler);
            sdb.on('fail', sdFailHandler);
            sdbHandlersInitialized = true;
        }

        if (areConnsClosed()) {
            nodeShutdown();
        } else {
            sdb.backoff();
        }
    }

    this._shutdown = function(callback) {
        runShutdown(this, callback);
    };

    this._stateCheck = function (allowedStates) {
        return utils.stateCheck(this.name, this.state,
            allowedStates, stateNames);
    };
}

util.inherits(RiakNode, events.EventEmitter);

/**
 * Start this RiakNode.
 * @method start
 * @param {Function} callback - a callback for when node is started.
 */
RiakNode.prototype.start = function(callback) {
    this._stateCheck([State.CREATED]);

    logger.debug('%s starting', this.name);

    // Fire up connection pool
    var funcs = [];
    for (var i = 0; i < this.minConnections; i++) {
        funcs.push(this._makeNewConnectionFunc(this));
    }

    var self = this;
    async.parallel(funcs, function (err, rslts) {
        if (err) {
            logger.error('%s (%d) error during start:', self.name, self.executeCount, err);
        }
        self._startIdleExpiration();
        self.state = State.RUNNING;
        logger.debug('%s started', self.name);
        self.emit(EVT_SC, self, self.state);
        if (callback) {
            callback(err, self);
        }
    });
};

/**
 * Stop this RiakNode.
 * @param {Function} callback - called when node completely stopped.
 * @method stop
 */
RiakNode.prototype.stop = function(callback) {
    this._stateCheck([State.RUNNING, State.HEALTH_CHECKING]);
    this._stopIdleExpiration();
    this.state = State.SHUTTING_DOWN;
    logger.debug("%s shutting down", this.name);
    this.emit(EVT_SC, this, this.state);
    this._shutdown(callback);
};

/**
 * Execute a command on this RiakNode.
 * @method execute
 * @param {Object} command - a command to execute.
 * @return {Boolean} - if this RiakNode accepted the command for execution.
 */
RiakNode.prototype.execute = function (command) {
    this._stateCheck([State.RUNNING, State.HEALTH_CHECKING]);

    logger.debug("%s executing command '%s'", this.name, command.name);

    var executed = false;
    if (this._executeAllowed()) {
        var conn = this._getConnectionFromPool(command.name);
        // conn will be undefined if there's no available connections.
        if (!conn) {
            if (this._createNewConnectionAllowed()) {
                var self = this;
                this._createNewConnection(function (newConn) {
                    logger.debug("%s executing command '%s' (new connection %d)",
                        self.name, command.name, self.executeCount);
                    // NB: state could have transitioned out of RUNNING in the time
                    // connection is established
                    if (self._executeAllowed() && newConn.execute(command)) {
                        self.executeCount++;
                        logger.debug("%s (%d) executed command: '%s'",
                            self.name, self.executeCount, command.name);
                    }
                }, function (err) {
                    logger.debug("%s (%d) command execution failed '%s'",
                        self.name, self.executeCount, command.name);
                    self._doHealthCheck();
                    self._maybeRetryCommand(command, function () {
                        command.onError(err);
                    });
                });
                // NB: returning true is the only option since
                // creating a new connection is async
                executed = true;
            } else {
                logger.debug('%s all connections in use and at max', this.name);
                executed = false;
            }
        } else {
            logger.debug("%s executing command '%s' (existing connection %d)",
                this.name, command.name, this.executeCount);
            if (conn.execute(command)) {
                this.executeCount++;
                logger.debug("%s (%d) executed command: '%s'",
                    this.name, this.executeCount, command.name);
            }
            // NB: returning true is the only option since
            // executing a command is async
            executed = true;
        }
    }
    return executed;
};

RiakNode.prototype._responseReceived = function (conn, command, code, decoded) {
    // NB: this function is similar to _receiveHealthCheck in RiakConnection
    logger.debug("%s command '%s' _responseReceived: %d", this.name, command.name, code);
    var self = this;
    function onError(err) {
        self.executeCount--;
        self._returnConnectionToPool(conn);
        self._maybeRetryCommand(command, function () {
            if (err.riakError) {
                command.onRiakError(decoded);
            } else {
                command.onError(err.msg);
            }
        });
    }
    function onSuccess() {
        // All of our responses that return multiple protobuf messages (streaming) use
        // a "done" field. Checking for it allows us to know when a streaming op is done and
        // return the connections to the pool before calling the callback.
        // Some responses will be empty (null body), so we also need to account for that.
        var hasDone = decoded ? decoded.hasOwnProperty('done') : false;
        if ((hasDone && decoded.done) || !hasDone) {
            self.executeCount--;
            self._returnConnectionToPool(conn);
            logger.debug('%s command %s complete (%d)',
                self.name, command.name, self.executeCount);
        }
        command.onSuccess(decoded);
    }
    var data = {
        conn: conn,
        command: command,
        code: code,
        decoded: decoded,
        shouldCallback: false
    };
    utils.handleRiakResponse(data, onError, onSuccess);
};

RiakNode.prototype._connectionClosed = function (conn) {
    this._decrementConnectionCount();
    // See if a command was being handled
    var command = conn.command;
    logger.debug("%s connection closed; command: '%s', in-flight: '%s'",
        this.name, command.name, conn.inFlight);
    // NB: if there is no executing command on this connection,
    // inFlight will be false
    if (conn.inFlight) {
        this.executeCount--;
        this._maybeRetryCommand(command, function () {
            command.onError("Connection closed while executing command");
        });
    }
    this._doHealthCheck();
};

RiakNode.prototype._maybeRetryCommand = function (command, errfunc) {
    var tries = command.remainingTries;
    command.remainingTries--;
    logger.debug("%s command %s remaining tries %d -> %d",
        this.name, command.name, tries, command.remainingTries);
    if (command.remainingTries > 0) {
        this.emit(EVT_RC, command, this);
    } else {
        errfunc();
    }
};

/**
 * The state of this node.
 *
 * If listeneing for stateChange events, a numeric value will be sent that
 * can be compared to:
 *
 *     RiakNode.State.CREATED
 *     RiakNode.State.RUNNING
 *     RiakNode.State.HEALTH_CHECKING
 *     RiakNode.State.SHUTTING_DOWN
 *     RiakNode.State.SHUTDOWN
 *
 * See: {{#crossLink "RiakNode/stateChange:event"}}stateChange{{/crossLink}}
 *
 * @property State
 * @type {Object}
 * @static
 * @final
 */
var State = Object.freeze({
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

var consts = Object.freeze({
    DefaultRemoteAddress : '127.0.0.1',
    DefaultRemotePort : 8087,
    DefaultMinConnections : 1,
    DefaultMaxConnections : 256,
    DefaultIdleTimeout : 10000,
    DefaultConnectionTimeout : 3000,
    DefaultRequestTimeout : 5000,
    DefaultHealthCheck : new Ping(function (){})
});

var schema = Joi.object().keys({
    remoteAddress: Joi.string().default(consts.DefaultRemoteAddress),
    remotePort: Joi.number().min(1).default(consts.DefaultRemotePort),
    minConnections: Joi.number().min(0).default(consts.DefaultMinConnections),
    maxConnections: Joi.number().min(0).default(consts.DefaultMaxConnections),
    idleTimeout: Joi.number().min(1000).default(consts.DefaultIdleTimeout),
    connectionTimeout: Joi.number().min(1).default(consts.DefaultConnectionTimeout),
    requestTimeout: Joi.number().min(1).default(consts.DefaultRequestTimeout),
    healthCheck: Joi.object().default(consts.DefaultHealthCheck),
    cork: Joi.boolean().default(true),
    externalLoadBalancer: Joi.boolean().default(false),
    auth: Joi.object().optional().keys({
        user: Joi.string().required(),
        password: Joi.string().allow(''),
        // https://nodejs.org/api/tls.html#tls_tls_createsecurecontext_details
        pfx: [Joi.string(), Joi.binary()],
        key: [Joi.string(), Joi.binary()],
        passphrase: Joi.string(),
        cert: [Joi.string(), Joi.binary()],
        ca: [Joi.string(), Joi.array().items(Joi.string(), Joi.binary())],
        crl: [Joi.string(), Joi.array().items(Joi.string(), Joi.binary())],
        rejectUnauthorized: Joi.boolean()
    }).xor('password', 'cert', 'pfx')
      .with('cert', 'key')
      .without('password', ['cert', 'pfx'])
});

/**
 * This event is fired whenever the state of the RiakNode changes.
 * @event stateChange
 * @param {Object} node - the RiakNode object whose state changed
 * @param {Number} state - the {{#crossLink "RiakNode/State:property"}}RiakNode.State{{/crossLink}}
 */
var EVT_SC = 'stateChange';

/**
 * This event is fired whenever a command fails and needs to be retried.
 * @event retryCommand
 * @param {Object} command - the command to retry
 * @param {RiakNode} node - this RiakNode
 */
var EVT_RC = 'retryCommand';

/**
 * A Builder for constructing RiakNode instances.
 *
 * Rather than having to manually construct the __options__ and instantiating
 * a RiakNode directly, this builder may be used.
 *
 *      var riakNode = new RiakNode.Builder().withRemotePort(9999).build();
 *
 * @class RiakNode.Builder
 * @constructor
 */
function Builder() {}

Builder.prototype = {
    /**
     * Set the remote address for the RiakNode.
     * @method withRemoteAddress
     * @param {String} address - IP or hostanme of the Riak node (__default:__ 127.0.0.1)
     * @return {RiakNode.Builder}
     */
    withRemoteAddress : function (address) {
        this.remoteAddress = address;
        return this;
    },
    /**
     * Set the remote port for this RiakNode.
     * @method withRemotePort
     * @param {Number} port - remote port of the Riak node (__default:__ 8087)
     * @return {RiakNode.Builder}
     */
    withRemotePort : function (port) {
        this.remotePort = port;
        return this;
    },
    /**
     * Set the minimum number of active connections to maintain.
     * These connections are exempt from the idle timeout.
     * @method withMinConnections
     * @param {Number} minConnections - number of connections to maintain (__default:__ 1)
     * @return {RiakNode.Builder}
     */
    withMinConnections : function (minConnections) {
        this.minConnections = minConnections;
        return this;
    },
    /**
     * Set the maximum number of connections allowed.
     * @method withMaxConnections
     * @param {Number} maxConnections - maximum number of connections to allow (__default:__ 10000)
     * @return {RiakNode.Builder}
     */
    withMaxConnections : function (maxConnections) {
        this.maxConnections = maxConnections;
        return this;
    },
    /**
     * Set the idle timeout used to reap inactive connections.
     * Any connection that has been idle for this amount of time
     * becomes eligible to be closed and discarded excluding the number
     * set via __withMinConnections()__.
     * @method withIdleTimeout
     * @param {Number} idleTimeout - the timeout in milliseconds (__default:__ 10000)
     * @return {RiakNode.Builder}
     */
    withIdleTimeout : function (idleTimeout) {
        this.idleTimeout = idleTimeout;
        return this;
    },
    /**
     * Set the connection timeout used when making new connections.
     * @method withConnectionTimeout
     * @param {Number} connectionTimeout - timeout in milliseconds (__default:__ 3000).
     * @return {RiakNode.Builder}
     */
    withConnectionTimeout : function (connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    },
    /**
     * Set the request timeout used when executing commands.
     * @method withRequestTimeout
     * @param {Number} requestTimeout - timeout in milliseconds (__default:__ 5000).
     * @return {RiakNode.Builder}
     */
    withRequestTimeout : function (requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    },
    /**
     * Set whether to use the cork/uncork socket functions.
     *
     * @method withCork
     * @param {Boolean} [cork=true] use cork/uncork. Default is true.
     * @chainable
     */
    withCork : function (cork) {
        this.cork = cork;
        return this;
    },
    /**
     * Set whether this RiakNode connects to an external load balancer.
     *
     * @method withExternalLoadBalancer
     * @param {Boolean} [externalLoadBalancer=true] connects to external load balancer. Default is false.
     * @chainable
     */
    withExternalLoadBalancer : function (externalLoadBalancer) {
        this.externalLoadBalancer = externalLoadBalancer;
        return this;
    },
    /**
     * Set the authentication information for connections made by this node.
     * @method withAuth
     * @param {Object} auth Set the authentication information for connections made by this node.
     * @param {String} auth.user Riak username.
     * @param {String} [auth.password] Riak password. Not required if using user cert.
     * @param {String|Buffer} [auth.pfx] A string or buffer holding the PFX or PKCS12 encoded private key, certificate and CA certificates.
     * @param {String|Buffer} [auth.key] A string holding the PEM encoded private key.
     * @param {String} [auth.passphrase]  A string of passphrase for the private key or pfx.
     * @param {String|Buffer} [auth.cert]  A string holding the PEM encoded certificate.
     * @param {String|String[]|Buffer[]} [auth.ca] Either a string or list of strings of PEM encoded CA certificates to trust.
     * @param {String|String[]|Buffer[]} [auth.crl] Either a string or list of strings of PEM encoded CRLs (Certificate Revocation List).
     * @param {Boolean} [auth.rejectUnauthorized] A boolean indicating whether a server should automatically reject clients with invalid certificates. Only applies to servers with requestCert enabled.     * @return {RiakNode.Builder}
     */
    withAuth : function (auth) {
        this.auth = auth;
        return this;
    },
    /**
     * Set the command to be used for a health check.
     *
     * If this RiakNode performs a health check, a new connection is made and
     * a command performed. The default is to send a {{#crossLink "Ping"}}{{/crossLink}}
     * command but any command can be used. If it completes successfully the
     * health check is considered a success.
     * @method withHealthCheck
     * @param {Object} healthCheck - a command to execute as a health check.
     * @chainable
     */
    withHealthCheck : function (healthCheck) {
        this.healthCheck = healthCheck;
        return this;
    },
    /**
     * Builds a RiakNode instance.
     * @method build
     * @return {RiakNode}
     */
    build : function () {
        return new RiakNode(this);
    }
};

/**
 * Static factory for constructing a set of RiakNodes.
 *
 * To create a set of RiakNodes with the same options:
 *
 *      var options = new RiakNode.Builder().withMinConnections(10);
 *      var nodes = RiakNode.buildNodes(['192.168.1.1', '192.168.1.2'], options);
 *
 * __options__ can be manually constructed or an instance of the Builder.
 *
 * @static
 * @method buildNodes
 * @param {String[]} addresses - an array of IP|hostname[:port]
 * @param {Object} [options] - the options to use for all RiakNodes.
 * @return {Array/RiakNode}
 */
var buildNodes = function (addresses, options) {
    var riakNodes = [];

    if (options === undefined) {
        options = {};
    }

    for (var i = 0; i < addresses.length; i++) {
        var split = addresses[i].split(':');
        options.remoteAddress = split[0];
        if (split.length === 2) {
            options.remotePort = split[1];
        }
        riakNodes.push(new RiakNode(options));
    }

    return riakNodes;
};

module.exports = RiakNode;
module.exports.buildNodes = buildNodes;
module.exports.Builder = Builder;
module.exports.State = State;
module.exports.consts = consts;
