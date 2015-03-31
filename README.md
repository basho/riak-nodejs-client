Riak NodeJS Client
==================

This is a **work in progress**. We are nearing completetion of what will be v1.0 of the Riak NodeJS client. 
Check back soon!

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

Travis-CI Build Status
----------------------

Authors
-------

Documentation
-------------

Release Notes
-------------

License
-------

The Riak NodeJS Client is Open Source software released under the Apache 2.0 License. Please see the [LICENSE](LICENSE) file for full license details.

Thanks
======

The following people have contributed to Riak NodeJS Client:

* Brian Roach
* Luke Bakken
* Bryce Kerley

