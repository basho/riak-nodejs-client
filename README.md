# Riak Node.js Client

**Riak Node.js Client** is a client which makes it easy to communicate with [Riak](http://basho.com/riak/), an open source, distributed database that focuses on high availability, horizontal scalability, and *predictable* latency. Both Riak and this code is maintained by [Basho](http://www.basho.com/).

## Build Status

* Master: [![Build Status](https://travis-ci.org/basho/riak-nodejs-client.svg?branch=master)](https://travis-ci.org/basho/riak-nodejs-client)

# Installation

`npm install basho-riak-client --save`

# Documentation

Most documentation is living in the [wiki](https://github.com/basho/riak-nodejs-client/wiki). For specifics on our progress here, see the [release notes](https://github.com/basho/riak-nodejs-client/blob/master/RELNOTES.md). 

# Testing / Contributing

This repository's maintainers are engineers at Basho and we welcome your contribution to the project! Review the details in [CONTRIBUTING.md](CONTRIBUTING.md) in order to give back to this project.

*Note:* Please clone this repository in such a manner that submodules are also cloned:

```
git clone --recursive https://github.com/basho/riak-nodejs-client

OR:

git clone https://github.com/basho/riak-nodejs-client
git submodule update --init --recursive
```

## Unit Tests

```sh
make unit-test
```

## Integration Tests

You have two options to run Riak locally - either build from source, or use a pre-installed Riak package.

### Source

To setup the default test configuration, build a Riak node from a clone of `github.com/basho/riak`:

```sh
# check out latest release tag
git checkout riak-2.1.4
make locked-deps
make rel
```

[Source build documentation](http://docs.basho.com/riak/kv/latest/setup/installing/source/).

When building from source, the protocol buffers port will be `8087` and HTTP will be `8098`.

### Package

Install using your platform's package manager ([docs](http://docs.basho.com/riak/kv/latest/setup/installing/))

When installing from a package, the protocol buffers port will be `8087` and HTTP will be `8098`.

### Running Integration Tests

* Ensure you've initialized this repo's submodules:

```sh
git submodule update --init
```

* Run the following:

```sh
./tools/setup-riak
make integration-test
```

# Roadmap

The **Riak Node.js Client** will support Node.js releases according to the [LTS schedule](https://github.com/nodejs/LTS#lts-schedule).

## License and Authors
**The Riak Node.js** Client is Open Source software released under the Apache 2.0 License. Please see the [LICENSE](LICENSE) file for full license details.

* Author: [Brian Roach](https://github.com/broach)
* Author: [Luke Bakken](http://bakken.io/)
* Author: [Bryce Kerley](https://github.com/bkerley)

## Contributors

Thank you to all of our contributors!

* [Tim Kennedy](https://github.com/stretchkennedy)
* [Doug Luce](https://github.com/dougluce) 
* [Charlie Zhang](https://github.com/charliezhang) 
* [Colin Hemmings](https://github.com/gonzohunter)
* [Timothy Stonis](https://github.com/tstonis)
* [Aleksandr Popov](https://github.com/mogadanez)
* [Josh Yudaken](https://github.com/qix)
* [Gabriel Nicolas Avellaneda](https://github.com/GabrielNicolasAvellaneda)
* [Iain Proctor](https://github.com/iproctor)
* [Brian Edgerton](https://github.com/brianedgerton)
* [Samuel](https://github.com/faust64)
* [Bryan Burgers](https://github.com/bryanburgers)
