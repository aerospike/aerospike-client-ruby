# Aerospike Gem

- [Usage](#usage)
- [Classes](#classes)
  - [Policies](#policies)
  - [Logger](#Logger)
  - [Client](#client)
  - [Key](#key)


<a name="usage"></a>
## Usage

The aerospike gem can be imported into your project via:

```ruby
require 'rubygems'
require 'aerospike'
```

<a name="classes"></a>
## Classes

<!--
################################################################################
Policies
################################################################################
-->
<a name="policies"></a>

### Policies

Policies contain the allowed values for policies for each of the [client](client.md) operations.

For details, see [Policies Object](policies.md)


<!--
################################################################################
Log
################################################################################
-->
<a name="logger"></a>

### Logger

For details, see [Logger Object](log.md)

<a name="client"></a>
### Client

#### Client.new(hosts, policy:, connect:)

Creates a new [client](client.md) with the provided configuration.

Parameters:

- `hosts`   – [optional] One or more `Aerospike::Host` objects, or a String
              with a comma separated list of hostnames. If no hosts are
              specified, the client will attempt to read the hostnames from the
              `AEROSPIKE_HOSTS` environment variable, or else default to "localhost:3000".
- `policy`  – [optional] Client policy to use (e.g. username/password, timeout, etc.)
- `connect` - [optional] Whether to connect to the cluster immediately. Default: true.

Returns a new client object.

#### TLS Encrypted Connections

Starting with Aerospike Enterprise Edition version 3.11, the server supports
Transport Layer Security (TLS) encryption for secure connections between the
clients and the cluster nodes. Please refer to the [TLS
Guide](https://www.aerospike.com/docs/guide/security/tls.html) for more
information on this feature.

To connect to an Aerospike cluster securely via an encrypted connection, you
need to configure TLS in the [`ClientPolicy`](policies.md#ClientPolicy) by
setting the required `ssl_options`.

#### IPv6

Aerospike Enterprise Edition version 3.10 and later support IPv6. Please refer
to the [IPv6
Configuration](https://www.aerospike.com/docs/operations/configure/network/ipv6/index.html)
section in the operations manual for how to configure your cluster to use IPv6.
To connect to a cluster with the Ruby client using IPv6, specify the IPv6 address
of one of more cluster nodes as the host seed address(es). Note that on the client side, when using IPv6 addresses in a hosts string, the IPv6 addresses must be enclosed in square brackets, e.g. `[fde4:8dba:82e1::c4]:3000`.

#### Examples

Specifying a single host seed address:

```ruby
  host = Aerospike::Host.new("127.0.0.1", 3000)
  client = Aerospike::Client.new(host)
```

Specifying a list of host addresses:

```ruby
  client = Aerospike::Client.new("10.0.0.1,10.0.0.2")
```

Specifying a different port:

```ruby
  client = Aerospike::Client.new("10.0.0.1:3500,10.0.0.2:3500")
```

Using IPv6:

```ruby
  client = Client.new("[fde4:8dba:82e1::c4]:3000")
```

Specifying a server's `tls-name` when using TLS encryption:

```ruby
  client = Client.new("10.0.0.1:mydomain:3000")
```

Using `AEROSPIKE_HOSTS` to set the hostnames:

```ruby
  ENV["AEROSPIKE_HOSTS"] = "10.0.0.1,10.0.0.2"
  client = Client.new
```


For details, see [Client Class](client.md).

<!--
################################################################################
key
################################################################################
-->
<a name="key"></a>

### Key(ns, set, key):

Creates a new [key object](datamodel.md#key) with the provided arguments.

Parameters:

- `ns`    – The namespace for the key.
- `set`   – The set for the key.
- `key`   – The value for the key.

Returns a new key instance.

Example:

```ruby
  key = Aerospike::Key.new("test", "demo", 123)
```

For details, see [Key Object](datamodel.md#key).
