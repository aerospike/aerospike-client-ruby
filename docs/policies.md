# Policies

Policies provide the ability to modify the behavior of operations.

This document provides information on the structure of policy objects for specific
operations and the allowed values for some of the policies.

- [`Policy Objects`](#Objects)
- [`Policy Values`](#Values)

<a name="Objects"></a>
## Objects

Policy objects are classes which define the behavior of associated operations.

When invoking an operation, you can choose:

- pass `nil` as policy. A relevant default policy will be used.
- pass policy in method options; e.g. `:ttl => 1`
- Generate the relevant policy object directly and pass it instead of the options parameter.

Usage Example:

```ruby
  client.get(key);
  client.get(key, [], policy.new);
```

<a name="ClientPolicy"></a>
### ClientPolicy Object

Client policy affecting the overall operation of the client. The client policy
needs to be set when a new client instance is being instantiated.

Attributes:

* `timeout` - Initial host connection timeout in seconds. The timeout when opening a connection
  to the server host for the first time.
  * Default: 1 second
* `connection_queue_size` - Size of the Connection Queue cache.
  * Default: 64
* `fail_if_not_connected` - Throw exception if host connection fails during the initial connection.
  * Default: `true`
* `tend_interval` - Tend interval in milliseconds; determines the interval at
  which the client checks for cluster state changes. Minimum interval is 10ms.
  * Default: 1000
* `user` - User name for clusters that require authentication (Enterprise only)
* `password` - Password for clusters that require authentication (Enterprise only)
* `cluster_name` - Cluster name (optional)
  * If specified, the cluster name will be verified whenever the client
    connects to a new cluster node and nodes with non-matching cluster name
    will be rejected.

<a name="Policy"></a>
### Policy Object

A policy affecting the behaviour of read operations.

Attributes:

* `priority` – Specifies the behavior for the key.
  * For values, see [Priority Values](#priority).
  * Default: `Priority.DEFAULT`
* `timeout` – Maximum time to wait for the operation to complete.
  * If 0 (zero), then the value means there will be no timeout enforced. Value
    should be in seconds.
  * Default: 0 (no timeout)
* `max_retries` – Number of times to try on connection errors.
  * Default: 2
* `sleep_between_retries` – Duration of waiting between retries.
  * Default: `0.500` (500ms)

<a name="WritePolicy"></a>
### WritePolicy Object

A policy affecting the behaviour of write operations.

Includes all [Policy](#Policy) attributes, plus:

* `send_key` – Qualify whether the server should store the record's primary key, or just use its digest.
  * Default: `true`
* `record_exists_action` – Qualify how to handle writes where the record already exists.
  * For values, see [RecordExistsAction Values](policies.md#exists).
  * Default: `RecordExistsAction.UPDATE`
* `generation_policy` – Qualify how to handle record writes based on record generation.
  * For values, see [GenerationPolicy Values](policies.md#gen).
  * Default: `GenerationPolicy.NONE` (generation is not used to restrict writes)
* `commit_level` – Desired consistency guarantee when committing a transaction on the server.
  * For values, see [CommitLevel Values](policies.md#commit).
  * Default: `CommitLevel.COMMIT_ALL` (wait for write confirmation from all replicas)
* `generation` – Expected generation.
  * Generation is the number of times a record has been modified (including
    creation) on the server. If a write operation is creating a record, the
    expected generation would be 0
  * Default: 0
* `expiration` – Record expiration. Also known as ttl (time to live).
  *  Seconds record will live before being removed by the server.
  * Expiration values:
      * -1: Never expire for Aerospike 2 server versions >= 2.7.2 and Aerospike 3 server versions >= 3.1.4. Do not use -1 for older servers.
      * 0: Default to namespace configuration variable "default-ttl" on the server.
      * > 0: Actual expiration in seconds.
  * Default: 0
* `durable_delete` - (boolean) If the transaction results in a record deletion, leave a tombstone for the record.
  This prevents deleted records from reappearing after node failures.
  * Valid for Aerospike Server Enterprise Edition 3.10+ only.
  Default: `false`

<a name="OperatePolicy"></a>
### OperatePolicy Object

A policy affecting the behavior of operate commands.

Includes all [WritePolicy](#WritePolicy) attributes, plus:

* `record_bin_multiplicity` - Specifies how to merge results from multiple operations on the same record bin.
  * Allowed values: See [RecordBinMultiplicity Values](#RecordBinMultiplicity)
  * Default: `RecordBinMultiplicity::SINGLE`

<a name="QueryPolicy"></a>
### QueryPolicy Object

A policy affecting the behaviour of query and scan operations.

Includes All Policy attributes, plus:

* `record_queue_size` - The record set buffers the query results locally.
  * This attribute controls the size of the buffer (a `SizedQueue` instance).
  * Default: 5000

<a name="Values"></a>
## Values

The following are values allowed for various policies.

<a name="gen"></a>
### GenerationPolicy Values

* `NONE` - Writes a record, regardless of generation.
* `EXPECT_GEN_EQUAL` - Writes a record, ONLY if generations are equal.
* `EXPECT_GEN_GT` - Writes a record, ONLY if local generation is greater-than remote generation.
* `DUPLICATE` - Writes a record creating a duplicate, ONLY if the generation collides.

<a name="exists"></a>
### RecordExistsAction Values

* `UPDATE` - Create or update record.
  * Merge write command bins with existing bins.
* `UPDATE_ONLY` - Update record only. Fail if record does not exist.
  * Merge write command bins with existing bins.
* `REPLACE` - Create or replace record.
  * Delete existing bins not referenced by write command bins.
  * Supported by Aerospike 2 server versions >= 2.7.5 and
    Aerospike 3 server versions >= 3.1.6.
* `REPLACE_ONLY` - Replace record only. Fail if record does not exist.
  * Delete existing bins not referenced by write command bins.
  * Supported by Aerospike 2 server versions >= 2.7.5 and
    Aerospike 3 server versions >= 3.1.6.
* `CREATE_ONLY` - Create only. Fail if record exists.

<a name="commit"></a>
### CommitLevel Values

* `COMMIT_ALL` - Wait until successfully committing master and all replicas.
* `COMMIT_MASTER`- Wait until successfully committing master only.

<a name="priority"></a>
### Priority Values

* `DEFAULT` - The server defines the priority.
* `LOW` - Run the database operation in a background thread.
* `MEDIUM` - Run the database operation at medium priority.
* `HIGH` - Run the database operation at the highest priority.

<a name="RecordBinMultiplicity"></a>
### RecordBinMultiplicity Values

* `SINGLE` - Returns only the value of the last operation on the bin.
* `ARRAY` - Returns all results of operations on the same bin as an array.
