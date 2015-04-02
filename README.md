Riak NodeJS Client
==================

Travis-CI Build Status
----------------------

[![Build Status](https://travis-ci.org/basho/riak-nodejs-client.svg?branch=master)](https://travis-ci.org/basho/riak-nodejs-client)

Cloning
-------

*Note:* Please use the `--recursive` git option or run `git submodule update --init` after cloning as a couple submodules are used. Thanks!

Building From Source
----------------------

* Ensure NodeJS 0.12 or later is installed
* Ensure the `npm` command is available.
* Clone and build it:

    ```
    git clone --recursive git://github.com/basho/riak-nodejs-client.git`
    cd riak-nodejs-client
    make install-deps
    make unit-test
    ```

Documentation
-------------

Please see the [wiki](https://github.com/basho/riak-nodejs-client/wiki).

Release Notes
-------------

Please see the [release
notes](https://github.com/basho/riak-nodejs-client/blob/master/RELNOTES.md).

License
-------

The Riak NodeJS Client is Open Source software released under the Apache 2.0 License. Please see the [LICENSE](LICENSE) file for full license details.

Thanks
======

The following people have contributed to Riak NodeJS Client:

* Brian Roach
* Luke Bakken
* Bryce Kerley

