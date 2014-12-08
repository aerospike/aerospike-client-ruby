# Introduction

This gem describes the Aerospike Ruby Client API in detail.


## Usage

The aerospike ruby client package is the main entry point to the client API.

```ruby
require 'rubygems'
require 'aerospike'
```

Before connecting to a cluster, you must import the package.

You can then generate a client object for connecting to and operating against a cluster.

```ruby
client = Aeropsike::Client.new("127.0.0.1", 3000)
```

The application will use the client object to connect to a cluster, then perform operations such as writing and reading records.
Client object is thread frinedly, so you can use it in other threads without synchronization.
It manages its connections and internal state automatically for optimal performance. These settings can also be tweaked.

For more details on client operations, see [Client Class](client.md).

## API Reference

- [Aerospike Ruby Client Library Overview](aerospike.md)
- [Client Class](client.md)
- [Object Model](datamodel.md)
- [Policy Objects](policies.md)
- [Logger Object](log.md)
