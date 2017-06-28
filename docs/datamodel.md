# Data Model

<!--
################################################################################
record
################################################################################
-->
<a name="record"></a>

## Record

A record is how the data is represented and stored in the database. A record is represented as a `class`.

Fields are:

- `bins`        — Bins and their values are represented as a hash. Hash keys should be of type string.
- `key`         — Associated Key.
- `node`        — Database node from which the record was retrieved from.
- `duplicates`  — If the `writepolicy.GenerationPolicy` is DUPLICATE, it will contain older versions of bin data.
- `ttl`         — Time-to-live (TTL) of the record in seconds. Shows in how many seconds the data will be erased if not updated.
- `generation`  — Record generation (number of times the record has been updated).

The keys of the Bins are the names of the fields (bins) of a record. The values for each field can be `nil`, `FixedNum`, `String`, `Array` or `Hash`.

```Note: Arrays and Maps can contain an array or a map as a value in them. In other words, nesting of complex values is allowed.```

Records are returned as a result of `get` operations. To write back their values, one needs to pass their bins field to the `put` method.

Simple example of a Read, Change, Update operation:

```ruby
  # define a client to connect to
  client = Client.new("127.0.0.1", 3000)

  key = Key.new("test", "demo", "key") # key can be of any supported type

  # define some bins
  bins = {
    "bin1" => 42, # you can pass any supported type as bin value
    "bin2" => "An elephant is a mouse with an operating system",
    "bin3" => ["Ruby", 2009],
  }

  # write the bins
  client.put(key, bins)

  # read it back!
  rec = client.get(key)

  # change data
  rec.bins["bin1"] += 1

  # update
  client.put(key, rec.bins)
```

<!--
################################################################################
key
################################################################################
-->
<a name="key"></a>

## Key.new(ns, set, key)

A record is addressable via its key. A key is a class containing:

- `ns`     — The namespace of the key. Must be a String.
- `set`    – The set of the key. Must be a String.
- `key`    – The value of the key. Can be of any supported types.

Example:

```ruby
  key = Key.new("test", "demo", "key") # key can be of any supported type
  
```

<!--
################################################################################
bin
################################################################################
-->
<a name="bin"></a>

## Bin.new(name, value)

Bins are analogous to columns in relational databases.

- `name`   — Bin name. Must be a String.
- `value`  – The value of the key. Can be of any supported type.

Example:

```ruby
  bin1 = Bin.new("name", "Aerospike") # string value
  bin2 = Bin.new("maxTPS", 1000000) # number value
  bin3 = Bin.new("notes", {
      "age" => 5,
      666: "not allowed in",
      "clients" => ["go", "c", "java", "python", "node", "erlang", 11, {"a" => "b"}],
    }) # go wild!
```
