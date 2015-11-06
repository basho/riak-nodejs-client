'use strict';

var Joi = require('joi');
var events = require('events');
var util = require('util');
var RiakConnection = require('./riakconnection');
var LinkedList = require('linkedlist');
var RiakProtobuf = require('../protobuf/riakprotobuf');
var logger = require('winston');
var Ping = require('../commands/ping');

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
 *       maxConnections: 10000,
 *       minConnections: 1,
 *       idleTimeout: 3000,
 *       connectionTimeout: 0,
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
 * @param {Number} [options.maxConnections=10000] Set the maximum number of connections allowed.
 * @param {Number} [options.idleTimeout=1000] Set the idle timeout used to reap inactive connections.
 * @param {Number} [options.connectionTimeout=0] Set the connection timeout used when making new connections.
 * @param {Object} [options.auth] Set the authentication information for connections made by this node.
 * @param {Boolean} [options.cork] Use 'cork' on all sockets. Default is true.
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

    Joi.validate(options, schema, function(err, options) {

        if (err) {
            throw err;
        }

        self.remoteAddress = options.remoteAddress;
        self.remotePort = options.remotePort;
        self.minConnections = options.minConnections;
        self.maxConnections = options.maxConnections;
        self.idleTimeout = options.idleTimeout;
        self.connectionTimeout = options.connectionTimeout;
        self.state = State.CREATED;
        self.auth = options.auth;
        self.cork = options.cork;
        self.healthCheck = options.healthCheck;
    });

    this._available = new LinkedList();
    this._currentNumConnections = 0;

}

util.inherits(RiakNode, events.EventEmitter);

/**
 * Start this RiakNode.
 * @method start
 */
RiakNode.prototype.start = function() {
    this._stateCheck([State.CREATED]);
    var self = this;

    logger.debug('[RiakNode] (%s:%s) starting', this.remoteAddress, this.remotePort); 

    // Fire up connection pool    
    for (var i = 0; i < this.minConnections; i++) {
        this._createNewConnection(this._returnConnectionToPool.bind(this), 
                                    function(){});
    }

    this._expireTimer = setInterval(function() {
       self._expireIdleConnections();
    }, 30000);

    this.state = State.RUNNING;
    logger.debug('[RiakNode] (%s:%d) started', this.remoteAddress, this.remotePort);
    this.emit(EVT_SC, this, this.state);
};

RiakNode.prototype._createNewConnection = function(postConnectFunc, postFailFunc, healthCheck) {

    this._currentNumConnections++; 
    var conn = new RiakConnection({
        remoteAddress : this.remoteAddress,
        remotePort : this.remotePort,
        connectionTimeout : this.connectionTimeout,
        auth: this.auth,
        healthCheck: healthCheck,
        cork: this.cork
    }); 

    var self = this;

    conn.on('connected', function(conn) {
        conn.on('responseReceived', self._responseReceived.bind(self));
        conn.on('connectionClosed', self._connectionClosed.bind(self));
        postConnectFunc(conn);
    });

    conn.on('connectFailed', function(conn, err){
       self._currentNumConnections--;
       postFailFunc(err);
    });

    conn.connect();

};

/**
 * Stop this RiakNode.
 * @method stop
 */
RiakNode.prototype.stop = function() {
    this._stateCheck([State.RUNNING, State.HEALTH_CHECKING]);

    clearInterval(this._expireTimer);
    this.state = State.SHUTTING_DOWN;
    logger.debug("[RiakNode] (%s:%d) shutting down.", this.remoteAddress, this.remotePort);
    this.emit(EVT_SC, this, this.state);
    this._shutdown();
};

RiakNode.prototype._shutdown = function() {

    while (this._available.next()) {
        var conn = this._available.removeCurrent();
        this._currentNumConnections--;
        conn.close();
    }
    this._available.resetCursor();

    if (this._currentNumConnections === 0) {
        this.state = State.SHUTDOWN;
        logger.debug("[RiakNode] (%s:%d) shut down.", this.remoteAddress, this.remotePort);
        this.emit(EVT_SC, this, this.state);
    } else {
        logger.debug("[RiakNode] (%s:%d); Connections still in use.", this.remoteAddress, this.remotePort);
        var self = this;
        this._shutdownTimer = setTimeout(function() {
            self._shutdown();
        }, 1000);
    }
};

/**
 * Execute a command on this RiakNode.
 * @method execute
 * @param {Object} command - a command to execute.
 * @return {Boolean} - if this RiakNode accepted the command for execution.
 */
RiakNode.prototype.execute = function(command) {
    this._stateCheck([State.RUNNING, State.HEALTH_CHECKING]);

    if (this.state === State.RUNNING) {
        var conn = this._available.shift();
        var self = this;
        // conn will be undefined if there's no available connections.
        if (!conn) {
            if (this._currentNumConnections < this.maxConnections) {

                this._createNewConnection(function(newConn) { 
                    logger.debug("[RiakNode] executing command '%s' on node (%s:%d)", command.PbRequestName, self.remoteAddress, self.remotePort);
                    newConn.execute(command);
                }, function(err) {
                    logger.debug("[RiakNode] command execution failed on node (%s:%d)", self.remoteAddress, self.remotePort);
                    if (self.state === State.RUNNING) {
                        self._doHealthCheck();
                    }
                    command.remainingTries--;
                    if (command.remainingTries) {
                        self.emit(EVT_RC, command, self);
                    } else {
                        command.onError(err);
                    }
                });

                return true;

            } else {
                logger.debug('[RiakNode] node (%s:%d): all connections in use and at max', this.remoteAddress, this.remotePort);
                return false;
            }
        } else {
            logger.debug("[RiakNode] executing command '%s' on node (%s:%d)", command.PbRequestName, this.remoteAddress, this.remotePort);
            conn.execute(command);
            return true;
        }
    } else {
        return false;
    }
};

RiakNode.prototype._responseReceived = function(conn, command, code, decoded) {

    command.remainingTries--;
    if (code === RiakProtobuf.getCodeFor('RpbErrorResp')) {
        this._returnConnectionToPool(conn);
        if (logger.debug) {
            var errmsg = decoded.getErrmsg().toString('utf8');
            var errcode = decoded.getErrcode();
            logger.debug("[RiakNode] node (%s:%d): command '%s' recevied RpbErrorResp (%d): %s",
                this.remoteAddress, this.remotePort, command.PbRequestName, errcode, errmsg);
        }
        if (command.remainingTries) {
            this.emit(EVT_RC, command, this);
        } else {
            command.onRiakError(decoded);
        }
    } else if (code !== command.getExpectedResponseCode()) {
        this._returnConnectionToPool(conn);
        var msg = '[RiakNode] node (' + this.remoteAddress + ':' +
                        this.remotePort + ') received wrong reponse; expected ' +
                        command.getExpectedResponseCode() +
                        ' received ' + code;
        logger.error(msg);
        if (command.remainingTries) {
            this.emit(EVT_RC, command, this);
        } else {
            command.onError(msg);
        }
    } else {
        // All of our responses that return multiple protobuf messages (streaming) use 
        // a "done" field. Checking for it allows us to know when a streaming op is done and
        // return the connections to the pool before calling the callback.
        // Some responses will be empty (null body), so we also need to account for that.
        var hasDone = decoded ? decoded.hasOwnProperty('done') : false;
        if ( (hasDone && decoded.done) || !hasDone) {
            logger.debug('[RiakNode] node (%s:%d): command %s complete',
                    this.remoteAddress, this.remotePort, command.PbRequestName);
            this._returnConnectionToPool(conn);
        }
        command.onSuccess(decoded);
    }

};

RiakNode.prototype._returnConnectionToPool = function(conn) {

    if (this.state < State.SHUTTING_DOWN) {
        conn.inFlight = false;
        conn.resetBuffer();
        this._available.unshift(conn);
        logger.debug("[RiakNode] node (%s:%d); Number of avail connections: %d", this.remoteAddress, this.remotePort, this._available.length);
    } else {
        logger.debug('[RiakNode] node (%s:%d); Connection returned to pool during shutdown.', this.remoteAddress, this.remotePort);
        this._currentNumConnections--;
        conn.close();
    }

};

RiakNode.prototype._connectionClosed = function(conn) {
    this._currentNumConnections--;
    // See if a command was being handled
    logger.debug("[RiakNode] node (%s:%d): Connection closed; inFlight: %d", this.remoteAddress, this.remotePort, conn.inFlight);
    if (conn.inFlight) {
        var command = conn.command;
        command.remainingTries--;
        if (command.remainingTries) {
            this.emit(EVT_RC, command, this);
        } else {
            command.onError("Connection closed while executing command");
        }
    }

    if (this.state !== State.SHUTTING_DOWN) {
        // PB connections don't time out. If one disconnects it's highly likely
        // the node went down or there's a network issue
        if (this.state !== State.HEALTH_CHECKING) {
            this._doHealthCheck();
        }
    }
};

RiakNode.prototype._stateCheck = function(allowedStates) {
    if (allowedStates.indexOf(this.state) === -1) {
        throw "RiakNode: Illegal State; required: " + allowedStates + " current: " + this.state; 
    }
};

RiakNode.prototype._doHealthCheck = function() {
    this.state = State.HEALTH_CHECKING;
    this.emit(EVT_SC, this, this.state);
    setImmediate(this._healthCheck.bind(this));
};

RiakNode.prototype._healthCheck = function() {

    var self = this;
    logger.debug("RiakNode (%s:%d) running health check", this.remoteAddress, this.remotePort);

    this._createNewConnection(function(newConn) {
        self._returnConnectionToPool(newConn);
        self.state = State.RUNNING;
        logger.debug("RiakNode (%s:%d) healthcheck success", self.remoteAddress, self.remotePort);
        self.emit(EVT_SC, self, self.state);
    }, function() {
        logger.debug("RiakNode (%s:%d) failed healthcheck.", self.remoteAddress, self.remotePort);
        // TODO: 30 secs seems too long
        setTimeout(function() { self._healthCheck(); }, 30000);
    }, this.healthCheck);

};

RiakNode.prototype._expireIdleConnections = function() {
    logger.debug("RiakNode (%s:%d) expiring idle connections", this.remoteAddress, this.remotePort);
    var now = Date.now();
    var count = 0;
    this._available.resetCursor();
    while (this._available.next() && this._currentNumConnections > this.minConnections) {
        if (now - this._available.current.lastUsed >= this.idleTimeout) {
            var conn = this._available.removeCurrent();
            this._currentNumConnections--;
            conn.close();
            count++;
        }
    }
    this._available.resetCursor();
    logger.debug("RiakNode (%s:%d) expired %d connections.", this.remoteAddress, this.remotePort, count);
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
var State = Object.freeze({ CREATED : 0, 
                            RUNNING : 1, 
                            HEALTH_CHECKING : 2, 
                            SHUTTING_DOWN : 3, 
                            SHUTDOWN : 4});

var defaultRemoteAddress = "127.0.0.1";
var defaultRemotePort = 8087;
var defaultMinConnections = 1;
var defaultMaxConnections = 10000; // magic number but close enough 
var defaultIdleTimeout = 3000;
var defaultConnectionTimeout = 0;
var defaultHealthCheck = new Ping(function(){});

var schema = Joi.object().keys({
    remoteAddress: Joi.string().default(defaultRemoteAddress),
    remotePort: Joi.number().min(1).default(defaultRemotePort),
    minConnections: Joi.number().min(0).default(defaultMinConnections),
    maxConnections: Joi.number().min(0).default(defaultMaxConnections),
    idleTimeout: Joi.number().min(1000).default(defaultIdleTimeout),
    connectionTimeout: Joi.number().min(1).default(defaultConnectionTimeout),
    healthCheck: Joi.object().default(defaultHealthCheck),
    cork: Joi.boolean().default(true),
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
    withRemoteAddress : function(address) {
        this.remoteAddress = address;
        return this;
    },

    /**
     * Set the remote port for this RiakNode.
     * @method withRemotePort
     * @param {Number} port - remote port of the Riak node (__default:__ 8087)
     * @return {RiakNode.Builder}
     */
    withRemotePort : function(port) {
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
    withMinConnections : function(minConnections) {
        this.minConnections = minConnections;
        return this;
    },

    /**
     * Set the maximum number of connections allowed.
     * @method withMaxConnections
     * @param {Number} maxConnections - maximum number of connections to allow (__default:__ 10000)
     * @return {RiakNode.Builder}
     */
    withMaxConnections : function(maxConnections) {
        this.maxConnections = maxConnections;
        return this;
    },

    /**
     * Set the idle timeout used to reap inactive connections.
     * Any connection that has been idle for this amount of time
     * becomes eligible to be closed and discarded excluding the number 
     * set via __withMinConnections()__.
     * @method withIdleTimeout
     * @param {Number} idleTimeout - the timeout in milliseconds (__defualt:__ 3000)
     * @return {RiakNode.Builder}
     */
    withIdleTimeout : function(idleTimeout) {
        this.idleTimeout = idleTimeout;
        return this;
    },

    /**
     * Set the connection timeout used when making new connections.
     * @method withConnectionTimeout
     * @param {Number} connectionTimeout - timeout in milliseconds (__default:__ 0).
     * @return {RiakNode.Builder}
     */
    withConnectionTimeout : function(connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    },

    /**
     * Set whether to use the cork/uncork socket functions.
     * 
     * @method withCork
     * @param {Boolean} [cork=true] use cork/uncork. Default is true.
     * @chainable
     */
    withCork : function(cork) {
        this.cork = cork;
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
    withAuth : function(auth) {
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
    withHealthCheck : function(healthCheck) {
        this.healthCheck = healthCheck;
        return this;
    },
    /**
     * Builds a RiakNode instance.
     * @method build
     * @return {RiakNode}
     */
    build : function() {
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
var buildNodes = function(addresses, options) {
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
