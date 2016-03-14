Release Notes
=============

* [2.1.2](https://github.com/basho/riak-nodejs-client/issues?q=milestone%3Ariak-nodejs-client-2.1.2] - Following issues / PRs addressed:
 * [Improve closed connection handling](https://github.com/basho/riak-nodejs-client/pull/139)
 * [Use TCP keep-alives if available](https://github.com/basho/riak-nodejs-client/issues/141)
 * [Update Winston dependency](https://github.com/basho/riak-nodejs-client/issues/132)
* 2.1.1 - Following issues / PRs addressed:
 * [Use well-known cipher list](https://github.com/basho/riak-nodejs-client/issues/126)
 * [Cipher list incompatible with Riak Security](https://github.com/basho/riak-nodejs-client/issues/104)
 * [Update `maxConnections` documentation](https://github.com/basho/riak-nodejs-client/issues/122)
* 2.1.0 - Following issues / PRs addressed:
 * [Refactor Joi validation](https://github.com/basho/riak-nodejs-client/pull/124)
 * [Add `Describe` command for Timeseries](https://github.com/basho/riak-nodejs-client/pull/123)
* 2.0.0 - Following issues / PRs addressed:
 * _NB_: Please ensure that the `.start()` and `.stop()` callback arguments are
 used when using the *Riak Node.js Client*. These functions will be called when
 your `Riak.Client` or `Riak.Cluster` objects are fully started or stopped.
 * [Improve `start` and `stop` callbacks](https://github.com/basho/riak-nodejs-client/pull/120)
* 1.9.1 - DEPRECATED RELEASE - DO NOT USE / Following issues / PRs addressed:
 * [Issue with starting `Client` object](https://github.com/basho/riak-nodejs-client/issues/118)
 * [PR for issue with starting `Client` object](https://github.com/basho/riak-nodejs-client/issues/119)
* 1.9.0 - DEPRECATED RELEASE - DO NOT USE / Following issues / PRs addressed:
 * [Time Series support](https://github.com/basho/riak-nodejs-client/pull/116)
 * [Update module dependencies](https://github.com/basho/riak-nodejs-client/issues/115)
 * [`RiakCluster` update nodes](https://github.com/basho/riak-nodejs-client/issues/114)
 * [Miscellaneous improvements](https://github.com/basho/riak-nodejs-client/issues/113)
 * [Fix timeout in TLS](https://github.com/basho/riak-nodejs-client/issues/112)
 * [Non-Time Series improvements](https://github.com/basho/riak-nodejs-client/issues/111)
 * [Add callbacks to `stop()` methods](https://github.com/basho/riak-nodejs-client/issues/110)
 * [Smarter node manager](https://github.com/basho/riak-nodejs-client/issues/110)
 * [Use correct cipher list](https://github.com/basho/riak-nodejs-client/issues/104)
* 1.8.1 - Re-publish due to `npm publish` error for `1.8.0`.
* 1.8.0 - Following issues / PRs addressed:
 * [Expose Get/Set bucket type properties on `Client`](https://github.com/basho/riak-nodejs-client/pull/103)
* 1.7.0 - Following issues / PRs addressed:
 * [Implement commands to Get/Set bucket type properties](https://github.com/basho/riak-nodejs-client/pull/98)
* 1.6.0 - Following issues / PRs addressed:
 * [Some code refactoring](https://github.com/basho/riak-nodejs-client/pull/97)
 * [Parameterize the queue submit interval of RiakCluster](https://github.com/basho/riak-nodejs-client/pull/96)
 * [Corrected method name in the Builder of RiakCluster](https://github.com/basho/riak-nodejs-client/pull/92)
 * [Corrected npm package name in documentation example](https://github.com/basho/riak-nodejs-client/pull/90)
* 1.5.1 - Following issues / PRs addressed:
 * [Fix uncaught exception in defaultnodemanager when nodes are removed](https://github.com/basho/riak-nodejs-client/pull/89)
 * [Allow binary Riak keys](https://github.com/basho/riak-nodejs-client/pull/87)
* 1.5.0 - Following issues / PRs addressed:
 * [JSON parse fails when tombstone is returned](https://github.com/basho/riak-nodejs-client/issues/74)
* 1.4.0 - Following issues / PRs addressed:
 * [Add "Content Encoding" to `RiakObject`](https://github.com/basho/riak-nodejs-client/pull/71)
* 1.3.0 - Following issues / PRs addressed:
 * [Small bug in `RiakCluster.removeNode`](https://github.com/basho/riak-nodejs-client/issues/68)
 * [Not found property should be consistent](https://github.com/basho/riak-nodejs-client/issues/65)
* 1.2.2 - Following issues / PRs addressed:
 * [Allow `NVal` in the `StoreIndex` command](https://github.com/basho/riak-nodejs-client/issues/60)
* 1.2.1 - Following issues / PRs addressed:
 * [Fix missing `client.js` file](https://github.com/basho/riak-nodejs-client/issues/59)
* 1.2 - Following issues / PRs addressed:
 * [Add `FetchPreflist` command](https://github.com/basho/riak-nodejs-client/pull/58)
 * [Use `cork` and `uncork` to send protobuf header and body](https://github.com/basho/riak-nodejs-client/pull/57)
 * [When creating a Yokozuna index, allow setting a timeout](https://github.com/basho/riak-nodejs-client/pull/55)
 * [Update to Riak protobuf 2.1.0.2](https://github.com/basho/riak-nodejs-client/pull/54)
* 1.1.3 - Following issues / PRs addressed:
 * [RiakCluster uses a global DefaultNodeManager](https://github.com/basho/riak-nodejs-client/issues/49)
* 1.1.2 - Following issues / PRs addressed:
 * [On cluster stop only emit state](https://github.com/basho/riak-nodejs-client/pull/46)
 * [vclock buffer issue](https://github.com/basho/riak-nodejs-client/issues/45)
 * [Fix check to determine if Riak should generate key](https://github.com/basho/riak-nodejs-client/pull/44)
* 1.1.1 - Following issues / PRs addressed:
 * [Add String detection to `setRegister()`](https://github.com/basho/riak-nodejs-client/pull/41)
 * [Remove `callback` from all Joi schemas](https://github.com/basho/riak-nodejs-client/pull/40)
* 1.1.0 - Following issues / PRs addressed:
 * [Command Queuing](https://github.com/basho/riak-nodejs-client/pull/38)
 * [Pluggable Node Selection](https://github.com/basho/riak-nodejs-client/pull/37)
 * [Add Links to `RiakObject`](https://github.com/basho/riak-nodejs-client/pull/35)
* 1.0.0 - Initial release with Riak 2.0 support.
