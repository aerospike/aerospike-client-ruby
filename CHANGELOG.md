# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [2.10.0] - 2019-05-10

* **New Features**
  * Add support for LB discovery / seeding. Thanks to [@filiptepper](https://github.com/filiptepper)! [[#80](https://github.com/aerospike/aerospike-client-ruby/issue/80)]

## [2.9.1] - 2019-04-03

* **Bug Fixes**
  * Query fails if one or more cluster nodes do not have records in the set [[#77](https://github.com/aerospike/aerospike-client-ruby/issue/77)]

* **Updates**
  * Change admin message version to 2 (from 0)
  * Remove unused BIN_EXISTS_ERROR (6) and BIN_NOT_FOUND (17) error codes
  * Tests: Support setting user/password when running specs

## [2.9.0] - 2018-11-09

* **New Features**
  * Add INFINITY and WILDCARD values for use in CDT map/list comparators. [AER-5945]

* **Bug Fixes**
  * Default policies set on Client instance do not get applied [[#74](https://github.com/aerospike/aerospike-client-ruby/issue/74)]

* **Updates**
  * *BREAKING CHANGE*: Change default for send_key write policy to false [[#73](https://github.com/aerospike/aerospike-client-ruby/issue/73)]
  * Support truncate info command argument "lut=now" for servers that require it. [AER-5955]

## [2.8.0] - 2018-08-06

* **New Features**
  * Support latest CDT List/Map server-side operations: [[#69](https://github.com/aerospike/aerospike-client-ruby/pull/69)]
    * Operations on Ordered Lists & Bounded Lists via new List Policy. (Requires server version v3.16.0 or later.)
    * Option to invert selection criteria for certain List/Map get/remove operations. (Requires server version v3.16.0 or later.)
    * List/Map index/rank relative get/remove operations. (Requires server version v4.3.0 or later.)
    * Partial list/map updates using PARTIAL / NO_FAIL write flags. (Requires server version v4.3.0 or later.)
  * Benchmarks: Output total TPS metrics at end of run [[#71](https://github.com/aerospike/aerospike-client-ruby/pull/71)]

* **Bug Fixes**
  * Check connection status of sockets retrieved from connection pool [[#72](https://github.com/aerospike/aerospike-client-ruby/pull/72)]

* **Updates**
  * Add JRuby back to CI test matrix [[#70](https://github.com/aerospike/aerospike-client-ruby/pull/70)]

## [2.7.0] - 2018-04-12

* **New Features**
  * Batch Index protocol support. Thanks to [@deenbandhu-agarwal](https://github.com/deenbandhu-agarwal)! [[#61](https://github.com/aerospike/aerospike-client-ruby/pull/61)]
  * Memory optimization: Avoid easy allocations. Thanks to [@wallin](https://github.com/wallin)! [[#62](https://github.com/aerospike/aerospike-client-ruby/pull/62)]
  * New node removal strategy. Thanks to [@wallin](https://github.com/wallin)! [[#63](https://github.com/aerospike/aerospike-client-ruby/pull/63)]
  * Support for IPv6. Requires Aerospike Enterprise Edition v3.10 or later. Thanks to [@wallin](https://github.com/wallin)! [[#65](https://github.com/aerospike/aerospike-client-ruby/pull/65)]

## [2.6.0] - 2018-03-27

* **New Features**
  * Support for peers protocol for cluster discovery. Requires Aerospike server version 3.10 or later. Thanks to [@wallin](https://github.com/wallin) of [castle.io](https://castle.io/)! [[#59](https://github.com/aerospike/aerospike-client-ruby/pull/59)]
  * TLS encryption support for client <-> server connections. Requires Aerospike Enterprise Edition version 3.11 or later. Thanks to [@wallin](https://github.com/wallin) of [castle.io](https://castle.io/)! [[#59](https://github.com/aerospike/aerospike-client-ruby/pull/59)]

* **Bug Fixes**
  * Fix min./max. boundary check for Integer bin values and improve performance. Thanks to [@wallin](https://github.com/wallin) of [castle.io](https://castle.io/)! [[#60](https://github.com/aerospike/aerospike-client-ruby/pull/60)]

* **Updates**
  * Update minimum required Ruby version to v2.3.

## [2.5.1] - 2018-01-25

* **Bug Fixes**
  * Some secondary index queries fail with parameter error on Aerospike Server v3.15.1.x [#57](https://github.com/aerospike/aerospike-client-ruby/issues/57)

* **Updates**
  * Added Ruby 2.5 to test matrix
  * Updated documentation for Client#truncate command [CLIENT-985]

## [2.5.0] - 2017-10-10

* **New Features**
  * Support nobins flag on query operations
  * Support CDT List Increment operation. Requires Aerospike server version 3.15 or later.

* **Updates**
  * The deprecated Large Data Types(LDT) feature has been removed.
  * Ruby 2.1 has been removed from the client's test matrix as [official support for Ruby 2.1 has ended in Apr 2017](https://www.ruby-lang.org/en/news/2017/04/01/support-of-ruby-2-1-has-ended/). Nothing has changed in the client that would break compatibility with Ruby 2.1 yet. But compatibility is not guaranteed for future client releases.

## [2.4.0] - 2017-04-06

* **New Features**
  * Support ns/set truncate command [#47](https://github.com/aerospike/aerospike-client-ruby/issues/47)
  * Support configurable scan socket write timeout [#46](https://github.com/aerospike/aerospike-client-ruby/issues/46)

* **Bug Fixes**
  * Fix "Digest::Base cannot be directly inherited in Ruby" [#45](https://github.com/aerospike/aerospike-client-ruby/issues/45)

* **Updates**
  * Ruby 2.0 has been removed from the client's test matrix as [official support for Ruby 2.0 has ended in Feb 2016](https://www.ruby-lang.org/en/news/2016/02/24/support-plan-of-ruby-2-0-0-and-2-1/). Nothing has changed in the client that would break compatibility with Ruby 2.0 yet. But compatibility is not guaranteed for future client releases. [#52](https://github.com/aerospike/aerospike-client-ruby/pull/52)

## [2.3.0] - 2017-01-04

* **Bug Fixes**
  * Fix BytesValue used as record key. [#42](https://github.com/aerospike/aerospike-client-ruby/issues/42)

* **Changes**
  * Deprecate unsupport key types - only integer, string and byte keys are supported. [#43](https://github.com/aerospike/aerospike-client-ruby/issues/43)

## [2.2.1] - 2016-11-14

* **New Features**
  * Added constants `Aerospike::TTL::*` for "special" TTL values, incl. Aerospike::TTL::DONT_UPDATE (requires Aerospike Server v3.10.1 or later)

* **Bug Fixes**
  * Fix "Add node failed: wrong number of arguments". [#41](https://github.com/aerospike/aerospike-client-ruby/issues/41)

## [2.2.0] - 2016-09-20

* **New Features**
  * Support for durable delete write policy [CLIENT-768]; requires Aerospike
    Server Enterprise Edition v3.10 or later.
  * Support Cluster Name verification [CLIENT-776]; requires Aerospike Server v3.10 or later.

* **Bug Fixes**
  * Fix error handling in node refresh during cluster tend.

* **Improvements**
  * Optionally return multiple results from read operations on same record bin.
    [#39](https://github.com/aerospike/aerospike-client-ruby/issues/39) Thanks
    to [@zingoba](https://github.com/zingoba).

* **Documentation**
  * Added note about potential issues with usage in Ruby on Rails with Phusion Passenger.
  * Amend/clean up documentation of client policies.

## [2.1.1] - 2016-08-16

* **Bug Fixes**
  * Fix incorrect expiration times on records fetched via batch_get or query operations. [#38](https://github.com/aerospike/aerospike-client-ruby/issues/38)

* **Improvements**
  * Add support for two new server error codes (23 & 24) introduced in Aerospike Server v3.9.1.
  * Records returned by batch_get operation should include the full key incl. the user key part.

## [2.1.0] - 2016-07-19

* **Fixes**
  * Fix a typo in the `max_retries` policy parameter name. [PR #37](https://github.com/aerospike/aerospike-client-ruby/pull/37) Thanks to [@murphyslaw](https://github.com/murphyslaw)!
  * Fix license identifier in gemspec.

* **Improvements**
  * Support for queries on Lists and Maps (keys & values)
  * Support for creating indexes on Lists and Maps [CLIENT-685]
  * Support GeoJSON values in Lists and Maps

## [2.0.0] - 2016-05-27

* **Breaking Changes** - Please refer to detailed list of [API changes](https://www.aerospike.com/docs/client/ruby/usage/incompatible.html#version-2-0-0) for further details.
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

## [1.0.12] - 2016-02-11

* **Fixes**:

  * Fixed syntax error in Client when raising exception for invalid bin key;
    thanks to [Ole Riesenberg](https://github.com/oleriesenberg) for the fix.
    [aerospike/aerospike-client-ruby#31]
  * Use UTF-8 as default encoding when writing/reading Strings from record
    bins; thanks to [fs-wu](https://github.com/fs-wu) for finding the issue and
    reporting it. [aerospike/aerospike-client-ruby#33]

## [1.0.11] - 2015-12-04

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

## [1.0.10] - 2015-09-22

  Major fix release.

  * **Fixes**:

    * Fixes `find_node_in_partition_map` logic.

    * Fixes `Node.Refresh` logic.

    * Fixes an issue with dead connections that would cause an infinite loop.

## [1.0.9] - 2015-08-11

  Minor fix release.

  * **Fixes**:

    * Sends the original key value to the server for all relevant commands, including `operate` and `execute_udf`

## [1.0.8] - 2015-07-23

  Minor fix release.

  * **Improvements**:

    * Adds 'Filter.to_s'. Thanks to [Ángel M](https://github.com/Angelmmiguel)

  * **Fixes**:

    * Fixes an issue in write policy that would use an undefined variable if `:send_key` isn't `nil`.

    * Fixes an issue in cluster.closed logic.

    * Fixes an issue with including the `statement.rb` in the manifest. Thanks to [Ángel M](https://github.com/Angelmmiguel)

## [1.0.7] - 2015-05-15

  Minor fixes.

  NOTICE: All LDTs on server other than LLIST have been deprecated, and will be removed in the future. As Such, all API regarding those features are considered deprecated and will be removed in tandem.

  * **Improvements**:

    * Removed workaround in `BatchGet`. Bins are filtered on server now.

    * Added New Error Codes. Fixes Issues #17 and #18

    * Node validator won't lookup hostsif an IP is passed as a seed to it.

  * ** Other Changes **

    * Removed deprecated `ReplaceRoles()` method.

    * Removed deprecated `SetCapacity()` and `GetCapacity()` methods for LDTs.

## [1.0.6] - 2015-04-02

  Minor fixes.

  * **New Features**:

    * Fixed running a stream query without parameters to the function.

## [1.0.5] - 2015-03-25

  Minor improvements.

  * **New Features**:

    * Added `:execute_udf_on_query` method to `Aerospike::Client`

## [1.0.4] - 2015-03-24

  Hot fix.

  * **Fixes**:

    * Close a socket if connection raises an exception to avoid leaking the file descriptor.

## [1.0.3] - 2015-03-24

  Minor fixes and improvements.

  * **New Features**:

    * Symbols are now accepted as key values. Keep in mind that symbols are converted to string automatically, and type information is lost.

  * **Fixes**:

    * Wait for a good connection on `socket.connect_nonblock` to prevent infinite loops on read/write operations.


## [1.0.2] - 2015-03-14

  Minor improvements.

  * **New Features**:

    * Added `:new_many` method to `Aerospike::Client`

## [1.0.1] - 2015-01-28

  Hot fix.

  * **Fixes**:

    * Added `bcrypt` to the gem dependencies.

## [1.0.0] - 2015-01-26

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

## [0.1.6] - 2014-12-28

  Minor features added, minor fixes and improvements.

  * **New Features**:

    * Added `Policy.consistency_level`
    * Added `WritePolicy.commit_level`

  * **Fixes**

    * Fixed setting timeout on connection
    * Fixed exception handling typo for Connection#write

## [0.1.5] - 2014-12-08

  Major features added, minor fixes and improvements.

  * **New Features**:

    * Added `Client.scan_node`, `Client.scan_all`
    * Added `Client.query`

  * **Fixes**

    * Fixed getting back results only for specified bin names.

## [0.1.3] - 2014-10-27

  Minor fix.

  * **Changes**:

    * Fixed LDT bin and module name packing.

## [0.1.2] - 2014-10-25

  Minor fix.

  * **Changes**:

    * Fixed String unpacking for single byte strings.

## [0.1.1] - 2014-10-25

  Minor fixes.

  * **Changes**:

    * Fixed String packing header in Hash and Array.
    * #find on LDTs returns `nil` instad of raising an exception if the item is not found.

## [0.1.0] - 2014-10-14

  * Initial Release.
