# Aerospike Ruby Client [![travis][travis-image]][travis-url] [![codecov][codecov-image]][codecov-url] [![gem][gem-image]][gem-url]

[travis-image]: https://travis-ci.org/aerospike/aerospike-client-ruby.svg?branch=master
[travis-url]: https://travis-ci.org/aerospike/aerospike-client-ruby
[codecov-image]: https://codecov.io/gh/aerospike/aerospike-client-ruby/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/aerospike/aerospike-client-ruby
[gem-image]: https://img.shields.io/gem/v/aerospike.svg
[gem-url]: https://rubygems.org/gems/aerospike

An Aerospike library for Ruby.

This library is compatible with Ruby 2.3+ and supports Linux, Mac OS X and various other BSDs.

- [Usage](#Usage)
- [Prerequisites](#Prerequisites)
- [Installation](#Installation)
- [Benchmarks](#Benchmarks)
- [API Documentaion](#API-Documentation)
- [Tests](#Tests)
- [Examples](#Examples)
  - [Tools](#Tools)


## Usage:

The following is a very simple example of CRUD operations in an Aerospike database.

```ruby
require 'rubygems'
require 'aerospike'

include Aerospike

client = Client.new("127.0.0.1")

key = Key.new('test', 'test', 'key value')
bin_map = {
  'bin1' => 'value1',
  'bin2' => 2,
  'bin4' => ['value4', {'map1' => 'map val'}],
  'bin5' => {'value5' => [124, "string value"]},
}

client.put(key, bin_map)
record = client.get(key)
record.bins['bin1'] = 'other value'

client.put(key, record.bins)
record = client.get(key)
puts record.bins

client.delete(key)
puts client.exists(key)

client.close
```

More examples illustrating the use of the API are located in the
[`examples`](examples) directory.

Details about the API are available in the [`docs`](docs) directory.

<a name="Prerequisites"></a>
## Prerequisites

[Ruby](https://ruby-lang.org) version v2.3+ is required.

Aerospike Ruby client implements the wire protocol, and does not depend on the C client.
It is thread friendly.

Supported operating systems:

- Major Linux distributions (Ubuntu, Debian, Redhat)
- Mac OS X
- other BSDs (untested)

<a name="Installation"></a>
## Installation

### Installation from Ruby gems

1. gem install aerospike

### Installation from source

1. Install Ruby 2.3+
2. Install RubyGems
3. Install Bundler: ```gem install bundler```
4. Install dependencies: ```bundler install```
5. Build and Install the gem locally: ```rake build && rake install```
6. Run the benchmark: ```./tools/benchmark/benchmark.rb -u```

<a name="Tests"></a>
## Tests

This library is packaged with a number of tests.

To run all the test cases:

    $ AEROSPIKE_HOSTS="<host:port>[,<hoist:port>]" AEROSPIKE_USER="<user>" AEROSPIKE_PASSWORD="<pass>" bundle exec rspec

<a name="Examples"></a>
## Examples

A variety of example applications are provided in the [`examples`](examples) directory.

<a name="Tools"></a>
### Tools

A variety of clones of original tools are provided in the [`tools`](tools) directory.
They show how to use more advanced features of the library to reimplement the same functionality in a more concise way.

<a name="Benchmarks"></a>
## Benchmarks

Benchmark utility is provided in the [`tools/benchmark`](tools/benchmark) directory.
See the [`tools/benchmark/README.md`](tools/benchmark/README.md) for details.

<a name="API-Documentation"></a>
## API Documentation

API documentation is available in the [`docs`](docs/README.md) directory.

## License

The Aerospike Ruby Client is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.

