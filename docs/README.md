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
client = Aeropsike::Client.new("127.0.0.1")
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

<a name="RubyOnRails"></a>
## Ruby on Rails

The Aerospike Ruby client can be used in Ruby on Rails. If you are using
Phusion Passenger or Unicorn and app pre-loading is enabled, it is important to
ensure that the client connects to the Aerospike cluster only after a new
application process was forked from the pre-loaded image.

E.g. in Passenger, the ["smart
spawning"](https://www.phusionpassenger.com/library/indepth/ruby/spawn_methods/)
method is enabled by default, which reduces startup time for new application
processes and can reduce memory usage. However, if the Aerospike client is
initialized and connects to the cluster as part of the application startup, the
network connections to the cluster nodes may be shared between different
application processes, leading to corruption of the network packets. See
[Smart spawning caveats](https://www.phusionpassenger.com/library/indepth/ruby/spawn_methods/#unintentional-file-descriptor-sharing)
for more information.

Both the Passenger and Unicorn application servers provide hooks that can be
used to connect to the Aerospike cluster once a new process was forked. E.g.
for Passenger, you can use the `:starting_worker_process` hook in
`config/initializers/aerospike.rb`:

```ruby
connect = true
if defined?(PhusionPassenger)
  connect = false
  PhusionPassenger.on_event(:starting_worker_process) do
    $as_client.connect
  end
end
$as_client = Aerospike::Client.new(connect: connect)
```
