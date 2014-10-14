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

### Client.new(host, port)

Creates a new [client](client.md) with the provided configuration.

Parameters:

- `name`   – Host name or IP to connect to.
- `port`   – Host port.

Returns a new client object.

Example:

```ruby
  client = Aerospike::Client.new("127.0.0.1", 3000)
```

For detals, see [Client Class](client.md).

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
