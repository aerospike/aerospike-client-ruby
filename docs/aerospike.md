# Aerospike Gem

- [Usage](#usage)
- [Classes](#classes)
  - [policies](#Policies)
  - [logger](#logger)
- [Classes](#classes)
  - [Client.new](#client)
  - [Key.new](#key)


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
<a name="Policies"></a>

### Policies

Policies contain the allowed values for policies for each of the [client](client.md) operations.

For details, see [Policies Object](policies.md)


<!--
################################################################################
Log
################################################################################
-->
<a name="Log"></a>

### logger

For details, see [Logger Object](log.md)

<a name="client"></a>

### Client.new(hosts, policy:, connect:)

Creates a new [client](client.md) with the provided configuration.

Parameters:

- `hosts`   – [optional] One or more `Aerospike::Host` objects, or a String
              with a comma separated list of hostnames. If no hosts are
              specified, the client will attempt to read the hostnames from the
              `AEROSPIKE_HOSTS` environment variable, or else default to "localhost:3000".
- `policy`  – [optional] Client policy to use (e.g. username/password, timeout, etc.)
- `connect` - [optional] Whether to connect to the cluster immediately. Default: true.

Returns a new client object.

Examples:

Specifying a single host seed address:

```ruby
  host = Aerospike::Host.new("127.0.0.1", 3000)
  client = Aerospike::Client.new(host)
```

Specifying a list of host addresses:

```ruby
  client = Aerospike::Client.new("10.0.0.1:3000,10.0.0.2:3100")
```

Using `AEROSPIKE_HOSTS` to set the hostnames:

```ruby
  ENV["AEROSPIKE_HOSTS"] = "192.168.10.10:3000"
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
