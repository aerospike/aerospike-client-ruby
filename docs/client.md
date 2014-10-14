# Client Class

The `Client` class provides methods which can be used to perform operations on an Aerospike
database cluster. In order to get an instance of the Client class, you need to initialize it:

```ruby
  client = Aerospike::Client.new("127.0.0.1", 3000)
```

To customize a Client behaviour:

```ruby
  client = Aerospike::Client.new("127.0.0.1", 3000, :connection_queue_size => 64, :timeout => 0.005)
```

*Notice*: Examples in the section are only intended to illuminate simple use cases without too much distraction. Always follow good coding practices in production.

With a new client, you can use any of the methods specified below:

- [Methods](#methods)
  - [#add](#add)
  - [#append](#append)
  - [#close](#close)
  - [#delete](#delete)
  - [#dxists](#exists)
  - [#batch_exists](#batchexists)
  - [#get](#get)
  - [#get_header](#getheader)
  - [#batch_get](#batchget)
  - [#batch_get_header](#batchgetheader)
  - [#connected?](#isConnected)
  - [#operate](#operate)
  - [#prepend](#prepend)
  - [#put](#put)
  - [#touch](#touch)
  - [#create_index](#createindex)
  - [#drop_index](#dropindex)
  - [#register_udf](#registerudf)
  - [#register_udf_from_file](#registerudffromfile)
  - [#execute_udf](#executeudf)


<a name="methods"></a>
## Methods

<!--
################################################################################
add()
################################################################################
-->
<a name="add"></a>
### add(key, bins, options={})

Using the provided key, adds values to the mentioned bins.
Bin value types should by of type `integer` for the command to have any effect.

Parameters:

- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – A hash used for specifying the fields and their corresponding values.
- `options`     – A hash representing [Write Policy Attributes](policies.md#WritePolicy) to use for this operation.
                  If not provided, ```@default_write_policy``` will be used.

Example:
```ruby
  key = Key.new("test", "demo", 123)

  bins = {
    "e" => 2,
    "pi" => 3,
  }

  client.add(nil, key, bins)
```

<!--
################################################################################
append()
################################################################################
-->
<a name="append"></a>

### append(key, bins, options={})

Using the provided key, appends provided values to the mentioned bins.
Bin value types should by of type `String` for the command to have any effect.

Parameters:

- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – A hash used for specifying the fields and their corresponding values.
- `options`     – A hash representing [Write Policy Attributes](policies.md#WritePolicy) to use for this operation.
                  If not provided, ```@default_write_policy``` will be used.

Example:
```ruby
  key = Key.new("test", "demo", 123)

  bins = {
    "story" => ", and lived happily ever after...",
  }

  client.append(key, bins)
```

<!--
################################################################################
close()
################################################################################
-->
<a name="close"></a>

### close

Closes the client connection to the cluster.

Example:
```ruby
  client.close
```

<!--
################################################################################
remove()
################################################################################
-->
<a name="delete"></a>

### delete(key, options={})

Removes a record with the specified key from the database cluster.

Parameters:

- `key`         – A [Key object](datamodel.md#key) used for locating the record to be removed.
- `options`     – A hash representing [Write Policy Attributes](policies.md#WritePolicy) to use for this operation.
                  If not provided, ```@default_write_policy``` will be used.

returned values:

- `existed`         – Boolean value that indicates if the Key existed.

Example:
```ruby
  key = Key.new("test", "demo", 123)

  if client.delete(key, :ttl => 0.005)
    # do something
  end
```

<!--
################################################################################
exists()
################################################################################
-->
<a name="exists"></a>

### exists(key, options={})

Using the key provided, checks for the existence of a record in the database cluster .

Parameters:

- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `options`     – A hash representing [Policy Attributes](policies.md#Policy) to use for this operation.
                  If not provided, ```@default_policy``` will be used.

Example:

```ruby
  key = Key.new("test", "demo", 123)

  if client.exists(key)
    # do something
  end
```

<!--
################################################################################
batchexists()
################################################################################
-->
<a name="batchexists"></a>

### batch_exists(keys, options={})

Using the keys provided, checks for the existence of records in the database cluster in one request.

Parameters:

- `keys`         – A [Key array](datamodel.md#key), used to locate the records in the cluster.
- `options`      – A hash representing [Policy Attributes](policies.md#Policy) to use for this operation.
                  If not provided, ```@default_policy``` will be used.

Example:

```ruby
  key1 = Key.new("test", "demo", 123)
  key2 = Key.new("test", "demo", 42)

  existance_array = client.batch_exists([key1, key2])
    # do something
  end
```

<!--
################################################################################
get()
################################################################################
-->
<a name="get"></a>

### get(key, bin_names=[], options={})

Using the key provided, reads a record from the database cluster .

Parameters:

- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bin_names`   – (optional) Bins to retrieve. Will retrieve all bins if not provided.
- `options`     – A hash representing [Policy Attributes](policies.md#Policy) to use for this operation.
                  If not provided, ```@default_policy``` will be used.

Example:

```ruby
  key = Key.new("test", "demo", 123)

  rec = client.get(key) # reads all the bins
```

<!--
################################################################################
getheader()
################################################################################
-->
<a name="getheader"></a>

### get_header(key, options={})

Using the key provided, reads record metadata *ONLY* from the database cluster. Record metadata includes record generation and Expiration (TTL from the moment of retrieval, in seconds)

```record.bins``` will always be empty in resulting ```record```.

Parameters:

- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `options`     – A hash representing [Policy Attributes](policies.md#Policy) to use for this operation.
                  If not provided, ```@default_policy``` will be used.

Example:

```ruby
  key = Key.new("test", "demo", 123)

  rec = client.get_header(key) # No bins will be retrieved
```

<!--
################################################################################
batchget()
################################################################################
-->
<a name="batchget"></a>

### batch_get(keys, bin_names=[], options={})

Using the keys provided, reads all relevant records from the database cluster in a single request.

Parameters:

- `keys`         – A [Key array](datamodel.md#key), used to locate the record in the cluster.
- `bin_names`    – (optional) Bins to retrieve. Will retrieve all bins if not provided.
- `options`     – A hash representing [Policy Attributes](policies.md#Policy) to use for this operation.
                  If not provided, ```@default_policy``` will be used.

Example:

```ruby
  key1 = Key.new("test", "demo", 123)
  key2 = Key.new("test", "demo", 42)

  recs = client.batch_get([key1, key2]) # reads all the bins
```

<!--
################################################################################
batchgetheader()
################################################################################
-->
<a name="batchgetheader"></a>

### batch_get_header(keys, options={})

Using the keys provided, reads all relevant record metadata from the database cluster in a single request.

```record.bins``` will always be empty in resulting ```record```.

Parameters:

- `keys`         – A [Key array](datamodel.md#key), used to locate the record in the cluster.
- `options`     – A hash representing [Policy Attributes](policies.md#Policy) to use for this operation.
                  If not provided, ```@default_policy``` will be used.

Example:

```ruby
  key1 = Key.new("test", "demo", 123)
  key2 = Key.new("test", "demo", 42)

  recs = client.batch_get_header([key1, key2]) # reads all the bins
```
<!--
################################################################################
idConnected()
################################################################################
-->
<a name="isConnected"></a>

### connected?

Checks if the client is connected to the cluster.

<!--
################################################################################
prepend()
################################################################################
-->
<a name="prepend"></a>

### prepend(key, bins. options={})

Using the provided key, prepends provided values to the mentioned bins.
Bin value types should by of `string` for the command to have any effect.

Parameters:

- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – A hash used for specifying the fields and value.
- `options`     – A hash representing [WritePolicy Attributes](policies.md#WritePolicy) to use for this operation.
                  If not provided, ```@default_write_policy``` will be used.

Example:
```ruby
  key = Key.new("test", "demo", 123)

  bins = {
    "story" => "Long ago, in a galaxy far far away, ",
  }

  client.prepend(key, bins)
```

<!--
################################################################################
put()
################################################################################
-->
<a name="put"></a>

### put(key, bins, options={})

Writes a record to the database cluster. If the record exists, it modifies the record with bins provided.
To remove a bin, set its value to `nil`.

Parameters:

- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `bins`        – A hash used for specifying the fields to store.
- `options`     – A hash representing [WritePolicy Attributes](policies.md#WritePolicy) to use for this operation.
                  If not provided, ```@default_write_policy``` will be used.

Example:
```ruby
  key = Key.new("test", "demo", 123)

  bins = {
    "a" => "Lack of skill dictates economy of style.",
    "b" => 123,
    "c" => []int{1, 2, 3},
    "d" => map[string]interface{}{"a" => 42, "b" => "An elephant is mouse with an operating system."},
  }

  client.put(key, bins, :ttl => 0.05) # ttl is set to 50ms
```

<!--
################################################################################
touch()
################################################################################
-->
<a name="touch"></a>

### touch(key, options={})

Create record if it does not already exist.
If the record exists, the record's time to expiration will be reset to the policy's expiration.

Parameters:

- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `options`     – A hash representing [WritePolicy Attributes](policies.md#WritePolicy) to use for this operation.
                  If not provided, ```@default_write_policy``` will be used.

Example:
```ruby
  key = Key.new("test", "demo", 123)

  client.touch(key, :ttl => 15) # 15 seconds
```

<!--
################################################################################
createindex()
################################################################################
-->
<a name="createindex"></a>

### create_index(namespace, set_name, index_name, bin_name, index_type. options={})

Creates a secondary index. ```create_index``` will return an ```IndexTask``` object which can be used to determine if the operation is completed asynchronously.

Parameters:

- `namespace`     – Namespace
- `set_name`      – Name of the Set
- `index_name`    – Name of index
- `bin_name`      – Bin name to create the index on
- `index_type`    – `:string` or `:numeric`
- `options`       – A hash representing [WritePolicy Attributes](policies.md#WritePolicy) to use for this operation.
                  If not provided, ```@default_write_policy``` will be used.

Example:

```ruby
  idx_task = client.create_index(nil, "test", "demo", "indexName", "binName", :numeric)

  idx_task.wait_till_completed
```

<!--
################################################################################
dropindex()
################################################################################
-->
<a name="dropindex"></a>
### drop_index(namespace, set_name, index_name, options={})

Drops an index.

Parameters:

- `namespace`         – Namespace
- `set_name`          – Name of the Set.
- `index_name`        – Name of index
- `options`           – A hash representing [WritePolicy Attributes](policies.md#WritePolicy) to use for this operation.
                      If not provided, ```@default_write_policy``` will be used.

```ruby
  client.drop_index("test", "demo", "indexName")
```

<!--
################################################################################
registerudf()
################################################################################
-->
<a name="registerudf"></a>

### register_udf(udf_body, server_path, language, options={})

Registers the given UDF on the server.

Parameters:

- `udf_body`     – UDF source code
- `server_path`  – Path on which the UDF should be put on the server-side
- `language`     – Only 'LUA' is currently supported
- `options`      – A hash representing [WritePolicy Attributes](policies.md#WritePolicy) to use for this operation.
                 If not provided, ```@default_write_policy``` will be used.


Example:

```ruby
  udf_body = "function testFunc1(rec)
     local ret = map()                     -- Initialize the return value (a map)

     local x = rec['bin1']               -- Get the value from record bin named "bin1"

     rec['bin2'] = (x / 2)               -- Set the value in record bin named "bin2"

     aerospike:update(rec)                -- Update the main record

     ret['status'] = 'OK'                   -- Populate the return status
     return ret                             -- Return the Return value and/or status
  end"

  reg_task = client.register_udf(udf_body, "udf1.lua", LUA)

  # wait until UDF is created
  reg_task.wait_till_completed
```

<!--
################################################################################
registerudffromfile()
################################################################################
-->
<a name="registerudffromfile"></a>

### register_udf_from_file(client_path, server_path, language, options={})

Read the UDF source code from a file and registers it on the server.

Parameters:
- `clientPath`    – full file path for UDF source code
- `server_path`   – Path on which the UDF should be put on the server-side
- `language`      – Only 'LUA' is currently supported
- `options`       – A hash representing [WritePolicy Attributes](policies.md#WritePolicy) to use for this operation.
                  If not provided, ```@default_write_policy``` will be used.

Example:

```ruby
  regTask = client.register_udf_from_file(nil, "~/path/udf.lua", "udf1.lua", LUA)

  # wait until UDF is created
  reg_task.wait_till_completed
```

<!--
################################################################################
execute()
################################################################################
-->
<a name="execute"></a>

### execute_udf(key, package_name, function_name, args, options={})

Executes a UDF on a record with the given key, and returns the results.

Parameters:

- `key`         – A [Key object](datamodel.md#key), used to locate the record in the cluster.
- `package_name`     – server path to the UDF
- `function_name`    – UDF name
- `args`            – (optional) UDF arguments
- `options`         – A hash representing [WritePolicy Attributes](policies.md#WritePolicy) to use for this operation.
                    If not provided, ```@default_write_policy``` will be used.

Example:

Considering the UDF registered in `register_udf` example above:

```ruby
    res = client.execute_udf(key, "udf1", "testFunc1")

    # res will be: {"status" => "OK"}
```
