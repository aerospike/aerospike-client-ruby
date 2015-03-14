# Aerospike Ruby Client

An Aerospike library for Ruby.

This library is compatible with Ruby 1.9.3+ and supports Linux, Mac OS X and various other BSDs. Rubinius is supported, but not JRuby - yet.


- [Usage](#Usage)
- [Prerequisites](#Prerequisites)
- [Installation](#Installation)
- [Tweaking Performance](#Performance)
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

client = Client.new("127.0.0.1", 3000)

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

[Ruby](https://ruby-lang.org) version v1.9.3+ is required.

Aerospike Ruby client implements the wire protocol, and does not depend on the C client.
It is thread friendly.

Supported operating systems:

- Major Linux distributions (Ubuntu, Debian, Redhat)
- Mac OS X
- other BSDs (untested)

<a name="Installation"></a>
## Installation from Ruby gems:

1. gem install aerospike

## Installation from source:

1. Install Ruby 1.9.3+
2. Install RubyGems
3. Install Bundler: ```gem install bundler```
4. Install dependencies: ```bundler install```
5. Build and Install the gem locally: ```rake build && rake install```
6. Run the benchmark: ```./tools/benchmark/benchmark.rb -u```

<a name="Performance"></a>
## Performance Tweaking

We are bending all efforts to improve the client's performance. In out reference benchmarks, Go client performs almost as good as the C client.

To read about performance variables, please refer to [`docs/performance.md`](docs/performance.md)

<a name="Tests"></a>
## Tests

This library is packaged with a number of tests.

To run all the test cases:

    $ bundle exec rspec


<a name="Examples"></a>
## Examples

A variety of example applications are provided in the [`examples`](examples) directory.
See the [`examples/README.md`](examples/README.md) for details.

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

