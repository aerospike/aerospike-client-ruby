HEAD
===================

* **Bug Fixes**
  * Fix error handling in node refresh during cluster tend

* **Documentation**
  * Added note about potential issues with usage in Ruby on Rails with Phusion Passenger

v2.1.1 / 2016-08-16
===================

* **Bug Fixes**
  * Fix incorrect expiration times on records fetched via batch_get or query operations. [#38](https://github.com/aerospike/aerospike-client-ruby/issues/38)

* **Improvements**
  * Add support for two new server error codes (23 & 24) introduced in Aerospike Server v3.9.1.
  * Records returned by batch_get operation should include the full key incl. the user key part.

v2.1.0 / 2016-07-19
===================

* **Fixes**
  * Fix a typo in the `max_retries` policy parameter name. [PR #37](https://github.com/aerospike/aerospike-client-ruby/pull/37) Thanks to [@murphyslaw](https://github.com/murphyslaw)!
  * Fix license identifier in gemspec.

* **Improvements**
  * Support for queries on Lists and Maps (keys & values)
  * Support for creating indexes on Lists and Maps [CLIENT-685]
  * Support GeoJSON values in Lists and Maps

v2.0.0 / 2016-05-27
===================

* **Breaking Changes** - Please refer to detailed list of [API changes](docs/api-changes.md#v2.0.0) for further details.
  * Incompatible integer key digests: digests for integer keys computed by v2 and v1 are different; the Aerospike server uses the key digest to retrieve records. This will impact your ability to read records with integer keys that were created by a v1 client version.
  * Backward incompatible changes to the `Aerospike::Client.new` initializer.
  * The `Aerospike::Client.new_many` initializer has been removed; use `Aerospike::Client.new` instead.
  * Drop support for Ruby 1.9.3; requires Ruby 2.0 or later.

* **Improvements**
  * Add support for List and Map operations on List/Map Complex Data Types (CDT); requires Aerospike Server version 3.9 or later. [CLIENT-559]
  * Read Aerospike server address from AEROSPIKE_HOSTS env variable if not specified explicity in client constructor.
  * Add 2.3.1 to supported Ruby versions on Travis-CI.

* **Fixes**
  * Fix digest creation for integer keys. [PR #34](https://github.com/aerospike/aerospike-client-ruby/pull/34). Thanks to [@murphyslaw](https://github.com/murphyslaw)!
  * Prevent "value must be enumerable" error when client cannot connect to cluster. [#35](https://github.com/aerospike/aerospike-client-ruby/issues/35). Thanks to [@rohanthewiz](https://github.com/rohanthewiz)!

1.0.12 / 2016-02-11
===================

* **Fixes**:

  * Fixed syntax error in Client when raising exception for invalid bin key;
    thanks to [Ole Riesenberg](https://github.com/oleriesenberg) for the fix.
    [aerospike/aerospike-client-ruby#31]
  * Use UTF-8 as default encoding when writing/reading Strings from record
    bins; thanks to [fs-wu](https://github.com/fs-wu) for finding the issue and
    reporting it. [aerospike/aerospike-client-ruby#33]

1.0.11 / 2015-12-04
===================

  Major feature and bug fix release.

  * **Fixes**:

    * Fix `ClientPolicy` to actually accept `fail_if_not_connected` parameter from constructor opts. PR #29, thanks to [Nick Recobra](https://github.com/oruen)

    * Fix record initialization issue. PR #28, thanks to [jzhua](https://github.com/jzhua)

    * Consume the rest of the stream when scan/query is finished.

  * **Improvements**:

    * Support for double precision floating point data type in record bins.
      Requires server version 3.6.0 or later. [CLIENT-599]

    * Support for geospatial data in record bins using GeoJSON format; support
      for querying geospatial indexes using points-within-region and
      region-contains-point filters. Requires server version 3.7.0 or later.
      [CLIENT-594]

    * Tend interval is now configurable via the client policy. Default is 1
      second as before.

    * Only logs tend messages when the number of cluster nodes have changed.

    * Scan and Query termination has been fixed.

1.0.10 / 2015-09-22
===================

  Major fix release.

  * **Fixes**:

    * Fixes `find_node_in_partition_map` logic.

    * Fixes `Node.Refresh` logic.

    * Fixes an issue with dead connections that would cause an infinite loop.

1.0.9 / 2015-08-11
==================

  Minor fix release.

  * **Fixes**:

    * Sends the original key value to the server for all relevant commands, including `operate` and `execute_udf`

1.0.8 / 2015-07-23
==================

  Minor fix release.

  * **Improvements**:

    * Adds 'Filter.to_s'. Thanks to [Ángel M](https://github.com/Angelmmiguel)

  * **Fixes**:

    * Fixes an issue in write policy that would use an undefined variable if `:send_key` isn't `nil`.

    * Fixes an issue in cluster.closed logic.

    * Fixes an issue with including the `statement.rb` in the manifest. Thanks to [Ángel M](https://github.com/Angelmmiguel)

1.0.7 / 2015-05-15
==================

  Minor fixes.

  NOTICE: All LDTs on server other than LLIST have been deprecated, and will be removed in the future. As Such, all API regarding those features are considered deprecated and will be removed in tandem.

  * **Improvements**:

    * Removed workaround in `BatchGet`. Bins are filtered on server now.

    * Added New Error Codes. Fixes Issues #17 and #18

    * Node validator won't lookup hostsif an IP is passed as a seed to it.

  * ** Other Changes **

    * Removed deprecated `ReplaceRoles()` method.

    * Removed deprecated `SetCapacity()` and `GetCapacity()` methods for LDTs.

1.0.6 / 2015-04-02
==================

  Minor fixes.

  * **New Features**:

    * Fixed running a stream query without parameters to the function.

1.0.5 / 2015-03-25
==================

  Minor improvements.

  * **New Features**:

    * Added `:execute_udf_on_query` method to `Aerospike::Client`

1.0.4 / 2015-03-24
==================

  Hot fix.

  * **Fixes**:

    * Close a socket if connection raises an exception to avoid leaking the file descriptor.

1.0.3 / 2015-03-24
==================

  Minor fixes and improvements.

  * **New Features**:

    * Symbols are now accepted as key values. Keep in mind that symbols are converted to string automatically, and type information is lost.

  * **Fixes**:

    * Wait for a good connection on `socket.connect_nonblock` to prevent infinite loops on read/write operations.


1.0.2 / 2015-03-14
==================

  Minor improvements.

  * **New Features**:

    * Added `:new_many` method to `Aerospike::Client`

1.0.1 / 2015-01-28
==================

  Hot fix.

  * **Fixes**:

    * Added `bcrypt` to the gem dependencies.

1.0.0 / 2015-01-26
==================

  Major release. With this release, Ruby client graduates to version 1.

  * **Breaking Changes**:

    * All `policy` initialize signatures have changed. Using policies was not documented, so it shouldn't affect most code. It will however, break any code initializing policies.
    * Removed `Record.dups` and `GenerationPolicy::DUPLICATE`

  * **New Features**:

    * Added Security Features: Please consult [Security Docs](https://www.aerospike.com/docs/guide/security.html) on Aerospike website.

      * `ClientPolicy.User`, `ClientPolicy.Password`
      * `Client.CreateUser()`, `Client.DropUser()`, `Client.ChangePassword()`
      * `Client.GrantRoles()`, `Client.RevokeRoles()`, `Client.ReplaceRoles()`
      * `Client.QueryUser()`, `Client.QueryUsers`

  * **Fixes**:

    * fixed size returned from `BytesValue.write`

0.1.6 / 2014-12-28
==================

  Minor features added, minor fixes and improvements.

  * **New Features**:

    * Added `Policy.consistency_level`
    * Added `WritePolicy.commit_level`

  * **Fixes**

    * Fixed setting timeout on connection
    * Fixed exception handling typo for Connection#write

0.1.5 / 2014-12-08
==================

  Major features added, minor fixes and improvements.

  * **New Features**:

    * Added `Client.scan_node`, `Client.scan_all`
    * Added `Client.query`

  * **Fixes**

    * Fixed getting back results only for specified bin names.

0.1.3 / 2014-10-27
==================

  Minor fix.

  * **Changes**:

    * Fixed LDT bin and module name packing.

0.1.2 / 2014-10-25
==================

  Minor fix.

  * **Changes**:

    * Fixed String unpacking for single byte strings.

0.1.1 / 2014-10-25
==================

  Minor fixes.

  * **Changes**:

    * Fixed String packing header in Hash and Array.
    * #find on LDTs returns `nil` instad of raising an exception if the item is not found.

0.1.0 / 2014-10-14
==================

  * Initial Release.
