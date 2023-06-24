# Changelog

All notable changes to this project will be documented in this file.

## [2.28.0] 2023-06-23
- **New Features**
  - [CLIENT-1432] Support minimum connections in connection pools

- **Updates**
  - [CLIENT-1529] Removed Policy.priority, ScanPolicy.scanPercent and ScanPolicy.failOnClusterChange

## [2.27.0] 2023-05-18
- **New Features**
  - [CLIENT-1176] Support write operations in background query


## [2.26.0] 2022-12-02

- **New Features**
  - [CLIENT-1808] Support creating a secondary index on elements within a CDT using `Context`.
  - [CLIENT-1991] Add base64 encoding methods to `Context`.
  - [CLIENT-2007] Support using `Context` in query filters.

## [2.25.0] 2022-11-28

- **New Features**

  - [CLIENT-1984] Support scan-show and query-show info commands.

  - [CLIENT-1362] Adds support Aerospike Expression filters. Expression filters are now supported on all commands, including `Client#get`, `Client#put`, `Client#delete`, `Client#operate`, `Client#scan`, `Client#query`, `Client#execute_udf`, etc.

    - Adds `Policy#filter_exp` and `Policy#fail_on_filtered_out`

    - Bit expressions: `Exp::Bit::` `#resize`, `#insert`, `#remove`, `#set`, `#or`, `#xor`, `#and`, `#not`, `#lshift`, `#rshift`, `#add`, `#subtract`, `#set_int`, `#get`, `#count`, `#lscan`, `#rscan`, `#get_int`, `#pack_math`, `#pack_get_int`, `#add_write`, `#add_read`

    - HLL Expressions: `Exp::HLL::` `#init`, `#add`, `#get_count`, `#get_union`, `#get_union_count`, `#get_intersect_count`, `#get_similarity`, `#describe`, `#may_contain`, `#add_write`, `#add_read`

    - Map Expressions: `Exp::Map::` `#put`, `#put_items`, `#increment`, `#clear`, `#remove_by_key`, `#remove_by_key_list`, `#remove_by_key_range`, `#remove_by_key_relative_index_range`, `#remove_by_value`, `#remove_by_value_list`, `#remove_by_value_range`, `#remove_by_value_relative_rank_range`, `#remove_by_value_relative_rank_range`, `#remove_by_index`, `#remove_by_index_range`, `#remove_by_rank`, `#remove_by_rank_range`, `#size`, `#get_by_key`, `#get_by_key_range`, `#get_by_key_list`, `#get_by_key_relative_index_range`, `#get_by_key_relative_index_range`, `#get_by_value`, `#get_by_value_range`, `#get_by_value_list`, `#get_by_value_relative_rank_range`, `#get_by_index`, `#get_by_index_range`, `#get_by_rank`, `#get_by_rank_range`, `#add_write`, `#add_read`, `#get_value_type`

    - List Expressions: `Exp::List::` `#append`, `#append_items`, `#insert`, `#insert_items`, `#increment`, `#set`, `#clear`, `#sort`, `#remove_by_value`, `#remove_by_value_list`, `#remove_by_value_range`, `#remove_by_value_relative_rank_range`, `#remove_by_index`, `#remove_by_index_range`, `#remove_by_rank`, `#remove_by_rank_range`, `#size`, `#get_by_value`, `#get_by_value_range`, `#get_by_value_list`, `#get_by_value_relative_rank_range`, `#get_by_index`, `#get_by_index_range`, `#get_by_index_range`, `#get_by_rank`, `#get_by_rank_range`, `#get_by_rank_range`, `#add_write`, `#add_read`, `#get_value_type`, `#pack_range_operation`

    - Read and Write operations: `Exp::Operation::` `#write`, `#read`

## [2.24.0] 2022-11-15

- **New Features**
  - [CLIENT-1730] Support partition queries.
  - [CLIENT-1469] Support query pagination through client#query_partitions with PartitionFilter
  - [CLIENT-1975] Add support for #max_records and #short_query to QueryPolicy
  - [CLIENT-1976] Add support for #concurrent_nodes to QueryPolicy

## [2.23.0] 2022-10-25

- **New Features**
  - [CLIENT-1752] Add 'EXISTS' return type for CDT read operations.
  - [CLIENT-1195] Support partition scans.
  - [CLIENT-1238] Support max_records on partition scans.
  - [CLIENT-1940] Lint and Clean up using Rubocop.

## [2.22.0] 2022-07-14

- **Fixes**
  - [CLIENT-1785] Fix Client#read_users to avoid error. PR #112 Thanks to [Dotan Mor](https://github.com/dotan-mor)
  - [CLIENT-1787] Support multiple DNS IPs during connection.
  - [CLIENT-1789] Authentication Retry fails in certain conditions.

## [2.21.1] - 2022-06-21

This s hotfix release. It is recommended to upgrade your client if you use authentication.

- **Bug Fixes**
  - Fix called function name in Authenticate.

## [2.21.0] - 2022-06-07

- **New Features**

  - Add support for new user management features. Adds `Client#query_role`, `Client#query_roles`, `Client#create_role`, `Client#drop_role`, `Client#grant_privileges`, `Client#revoke_privileges`. Adds the 'Role' class. Adds `UserRoles#read_info`, `UserRoles#write_info`, `UserRoles#conns_in_use` to the `UserRoles` class.

- **Improvements**
  - Do not run PredExp tests for server v6+.

## [2.20.1] - 2022-05-11

- **Improvements**
  - Add basic support for the new authentication protocol.

## [2.20.0] - 2021-11-08

Notice: This version of the client only supports Aerospike Server v4.9 and later. Some features will work for the older server versions, but they are not tested, nor officially supported.

- **New Features**

  - [CLIENT-1467] Support native Boolean type for server v5.6+.

- **Improvements**
  - Add basic support for server v4.9+ scan/queries.
  - Don't check for key equality in Batch command results.

## [2.19.0] - 2020-02-09

- **Improvements**
  - Remove Timeout in `wait_till_stabilized` in favor of `thread.join(timeout)` . Thanks to [Marcelo](https://github.com/MarcPer) [[#104](https://github.com/aerospike/aerospike-client-ruby/pull/104)]
  - Adds `ResultCode::LOST_CONFLICT`

## [2.18.0] - 2020-12-01

- **Bug Fixes**

  - Avoid panic if `Command#get_node` fails in `Command#execute`. Resolves issue #101.
  - Fix wrong method invocation inside `Client#truncate` method. Thanks to [Alexander](https://github.com/selivandex)

- **Improvements**
  - Added missing server error codes.

## [2.17.0] - 2020-10-15

- **New Features**

  - [CLIENT-1246] Adds missing API for Context#list_index_create and Context#map_key_create

- **Bug Fixes**
  - Fixed an issue were MsgPack extensions were not recursively cleared from the CDTs during unpacking.

## [2.16.0] - 2020-10-12

- **New Features**

  - [CLIENT-1173], [CLIENT-1246] Support Nested CDT operations with Context.
  - [CLIENT-1179], Support Bitwise operations.

- **Changes**
  - `ListSortFlags` now has an `ASCENDING` option, with `DEFAULT` mapping to it.

## [2.15.0] - 2020-10-05

- **New Features**

  - [CLIENT-1254] Adds support for HyperLogLog.

- **Changes**
  - `Client#operate` now uses `OperatePolicy` by default.

## [2.14.0] - 2020-08-06

- **New Features**

  - Adds support for rack-aware reads.
  - Adds support for client-server compression.

- **Improvements**
  - Adds support for `truncate-namespace` command.

## [2.13.0] - 2020-07-17

- **New Features**

  - Adds support for replica policies.

- **Improvements**
  - Remove support for "old" partition tokenizer.
  - Refactor how partition parser is initialized and called.
  - Adds support for 'replicas' and remove the old partition table queries from the server.

## [2.12.0] - 2019-04-21

- **New Features**

  - Support for predicate expressions in all transaction.
  - Support for `operation.delete` in `client#operate`.

- **Improvements**

  - Optimize serialization for nested structures. Thanks to [@Kacper Madej](https://github.com/madejejej)! [[#94](https://github.com/aerospike/aerospike-client-ruby/pull/94)]
  - Remove `Thread#abort_on_exception` from `batch_index_command`. Thanks to [@Kacper Madej](https://github.com/madejejej)! [[#92](https://github.com/aerospike/aerospike-client-ruby/pull/92)]
  - Does not allow values other than Integer, Float, String, Symbol and nil to be used as keys in Maps.

- **Bug Fixes**
  - Fixes tests that weren't using ENV variables for connections. This will allow the tests to be run on any server.

## [2.11.0] - 2019-05-17

- **New Features**

  - Support for predicate expressions in queries. Thanks to [@Minus10Degrees](https://github.com/Minus10Degrees)! [[#78](https://github.com/aerospike/aerospike-client-ruby/issues/78)]

- **Bug Fixes**
  - Client#execute_udf_on_query should not modify the statement argument. [[#79](https://github.com/aerospike/aerospike-client-ruby/issues/79)]
  - Encoding::UndefinedConversionError when reading blob data from CDT list/map bin. [[#84](https://github.com/aerospike/aerospike-client-ruby/issues/84)]

## [2.10.0] - 2019-05-10

- **New Features**
  - Add support for LB discovery / seeding. Thanks to [@filiptepper](https://github.com/filiptepper)! [[#80](https://github.com/aerospike/aerospike-client-ruby/issues/80)]

## [2.9.1] - 2019-04-03

- **Bug Fixes**

  - Query fails if one or more cluster nodes do not have records in the set [[#77](https://github.com/aerospike/aerospike-client-ruby/issues/77)]

- **Updates**
  - Change admin message version to 2 (from 0)
  - Remove unused BIN_EXISTS_ERROR (6) and BIN_NOT_FOUND (17) error codes
  - Tests: Support setting user/password when running specs

## [2.9.0] - 2018-11-09

- **New Features**

  - Add INFINITY and WILDCARD values for use in CDT map/list comparators. [AER-5945]

- **Bug Fixes**

  - Default policies set on Client instance do not get applied [[#74](https://github.com/aerospike/aerospike-client-ruby/issues/74)]

- **Updates**
  - _BREAKING CHANGE_: Change default for send_key write policy to false [[#73](https://github.com/aerospike/aerospike-client-ruby/issues/73)]
  - Support truncate info command argument "lut=now" for servers that require it. [AER-5955]

## [2.8.0] - 2018-08-06

- **New Features**

  - Support latest CDT List/Map server-side operations: [[#69](https://github.com/aerospike/aerospike-client-ruby/pull/69)]
    - Operations on Ordered Lists & Bounded Lists via new List Policy. (Requires server version v3.16.0 or later.)
    - Option to invert selection criteria for certain List/Map get/remove operations. (Requires server version v3.16.0 or later.)
    - List/Map index/rank relative get/remove operations. (Requires server version v4.3.0 or later.)
    - Partial list/map updates using PARTIAL / NO_FAIL write flags. (Requires server version v4.3.0 or later.)
  - Benchmarks: Output total TPS metrics at end of run [[#71](https://github.com/aerospike/aerospike-client-ruby/pull/71)]

- **Bug Fixes**

  - Check connection status of sockets retrieved from connection pool [[#72](https://github.com/aerospike/aerospike-client-ruby/pull/72)]

- **Updates**
  - Add JRuby back to CI test matrix [[#70](https://github.com/aerospike/aerospike-client-ruby/pull/70)]

## [2.7.0] - 2018-04-12

- **New Features**
  - Batch Index protocol support. Thanks to [@deenbandhu-agarwal](https://github.com/deenbandhu-agarwal)! [[#61](https://github.com/aerospike/aerospike-client-ruby/pull/61)]
  - Memory optimization: Avoid easy allocations. Thanks to [@wallin](https://github.com/wallin)! [[#62](https://github.com/aerospike/aerospike-client-ruby/pull/62)]
  - New node removal strategy. Thanks to [@wallin](https://github.com/wallin)! [[#63](https://github.com/aerospike/aerospike-client-ruby/pull/63)]
  - Support for IPv6. Requires Aerospike Enterprise Edition v3.10 or later. Thanks to [@wallin](https://github.com/wallin)! [[#65](https://github.com/aerospike/aerospike-client-ruby/pull/65)]

## [2.6.0] - 2018-03-27

- **New Features**

  - Support for peers protocol for cluster discovery. Requires Aerospike server version 3.10 or later. Thanks to [@wallin](https://github.com/wallin) of [castle.io](https://castle.io/)! [[#59](https://github.com/aerospike/aerospike-client-ruby/pull/59)]
  - TLS encryption support for client <-> server connections. Requires Aerospike Enterprise Edition version 3.11 or later. Thanks to [@wallin](https://github.com/wallin) of [castle.io](https://castle.io/)! [[#59](https://github.com/aerospike/aerospike-client-ruby/pull/59)]

- **Bug Fixes**

  - Fix min./max. boundary check for Integer bin values and improve performance. Thanks to [@wallin](https://github.com/wallin) of [castle.io](https://castle.io/)! [[#60](https://github.com/aerospike/aerospike-client-ruby/pull/60)]

- **Updates**
  - Update minimum required Ruby version to v2.3.

## [2.5.1] - 2018-01-25

- **Bug Fixes**

  - Some secondary index queries fail with parameter error on Aerospike Server v3.15.1.x [#57](https://github.com/aerospike/aerospike-client-ruby/issues/57)

- **Updates**
  - Added Ruby 2.5 to test matrix
  - Updated documentation for Client#truncate command [CLIENT-985]

## [2.5.0] - 2017-10-10

- **New Features**

  - Support nobins flag on query operations
  - Support CDT List Increment operation. Requires Aerospike server version 3.15 or later.

- **Updates**
  - The deprecated Large Data Types(LDT) feature has been removed.
  - Ruby 2.1 has been removed from the client's test matrix as [official support for Ruby 2.1 has ended in Apr 2017](https://www.ruby-lang.org/en/news/2017/04/01/support-of-ruby-2-1-has-ended/). Nothing has changed in the client that would break compatibility with Ruby 2.1 yet. But compatibility is not guaranteed for future client releases.

## [2.4.0] - 2017-04-06

- **New Features**

  - Support ns/set truncate command [#47](https://github.com/aerospike/aerospike-client-ruby/issues/47)
  - Support configurable scan socket write timeout [#46](https://github.com/aerospike/aerospike-client-ruby/issues/46)

- **Bug Fixes**

  - Fix "Digest::Base cannot be directly inherited in Ruby" [#45](https://github.com/aerospike/aerospike-client-ruby/issues/45)

- **Updates**
  - Ruby 2.0 has been removed from the client's test matrix as [official support for Ruby 2.0 has ended in Feb 2016](https://www.ruby-lang.org/en/news/2016/02/24/support-plan-of-ruby-2-0-0-and-2-1/). Nothing has changed in the client that would break compatibility with Ruby 2.0 yet. But compatibility is not guaranteed for future client releases. [#52](https://github.com/aerospike/aerospike-client-ruby/pull/52)

## [2.3.0] - 2017-01-04

- **Bug Fixes**

  - Fix BytesValue used as record key. [#42](https://github.com/aerospike/aerospike-client-ruby/issues/42)

- **Changes**
  - Deprecate unsupport key types - only integer, string and byte keys are supported. [#43](https://github.com/aerospike/aerospike-client-ruby/issues/43)

## [2.2.1] - 2016-11-14

- **New Features**

  - Added constants `Aerospike::TTL::*` for "special" TTL values, incl. Aerospike::TTL::DONT_UPDATE (requires Aerospike Server v3.10.1 or later)

- **Bug Fixes**
  - Fix "Add node failed: wrong number of arguments". [#41](https://github.com/aerospike/aerospike-client-ruby/issues/41)

## [2.2.0] - 2016-09-20

- **New Features**

  - Support for durable delete write policy [CLIENT-768]; requires Aerospike
    Server Enterprise Edition v3.10 or later.
  - Support Cluster Name verification [CLIENT-776]; requires Aerospike Server v3.10 or later.

- **Bug Fixes**

  - Fix error handling in node refresh during cluster tend.

- **Improvements**

  - Optionally return multiple results from read operations on same record bin.
    [#39](https://github.com/aerospike/aerospike-client-ruby/issues/39) Thanks
    to [@zingoba](https://github.com/zingoba).

- **Documentation**
  - Added note about potential issues with usage in Ruby on Rails with Phusion Passenger.
  - Amend/clean up documentation of client policies.

## [2.1.1] - 2016-08-16

- **Bug Fixes**

  - Fix incorrect expiration times on records fetched via batch_get or query operations. [#38](https://github.com/aerospike/aerospike-client-ruby/issues/38)

- **Improvements**
  - Add support for two new server error codes (23 & 24) introduced in Aerospike Server v3.9.1.
  - Records returned by batch_get operation should include the full key incl. the user key part.

## [2.1.0] - 2016-07-19

- **Fixes**

  - Fix a typo in the `max_retries` policy parameter name. [PR #37](https://github.com/aerospike/aerospike-client-ruby/pull/37) Thanks to [@murphyslaw](https://github.com/murphyslaw)!
  - Fix license identifier in gemspec.

- **Improvements**
  - Support for queries on Lists and Maps (keys & values)
  - Support for creating indexes on Lists and Maps [CLIENT-685]
  - Support GeoJSON values in Lists and Maps

## [2.0.0] - 2016-05-27

- **Breaking Changes** - Please refer to detailed list of [API changes](https://www.aerospike.com/docs/client/ruby/usage/incompatible.html#version-2-0-0) for further details.

  - Incompatible integer key digests: digests for integer keys computed by v2 and v1 are different; the Aerospike server uses the key digest to retrieve records. This will impact your ability to read records with integer keys that were created by a v1 client version.
  - Backward incompatible changes to the `Aerospike::Client.new` initializer.
  - The `Aerospike::Client.new_many` initializer has been removed; use `Aerospike::Client.new` instead.
  - Drop support for Ruby 1.9.3; requires Ruby 2.0 or later.

- **Improvements**

  - Add support for List and Map operations on List/Map Complex Data Types (CDT); requires Aerospike Server version 3.9 or later. [CLIENT-559]
  - Read Aerospike server address from AEROSPIKE_HOSTS env variable if not specified explicity in client constructor.
  - Add 2.3.1 to supported Ruby versions on Travis-CI.

- **Fixes**
  - Fix digest creation for integer keys. [PR #34](https://github.com/aerospike/aerospike-client-ruby/pull/34). Thanks to [@murphyslaw](https://github.com/murphyslaw)!
  - Prevent "value must be enumerable" error when client cannot connect to cluster. [#35](https://github.com/aerospike/aerospike-client-ruby/issues/35). Thanks to [@rohanthewiz](https://github.com/rohanthewiz)!

## [1.0.12] - 2016-02-11

- **Fixes**:

  - Fixed syntax error in Client when raising exception for invalid bin key;
    thanks to [Ole Riesenberg](https://github.com/oleriesenberg) for the fix.
    [aerospike/aerospike-client-ruby#31]
  - Use UTF-8 as default encoding when writing/reading Strings from record
    bins; thanks to [fs-wu](https://github.com/fs-wu) for finding the issue and
    reporting it. [aerospike/aerospike-client-ruby#33]

## [1.0.11] - 2015-12-04

Major feature and bug fix release.

- **Fixes**:

  - Fix `ClientPolicy` to actually accept `fail_if_not_connected` parameter from constructor opts. PR #29, thanks to [Nick Recobra](https://github.com/oruen)

  - Fix record initialization issue. PR #28, thanks to [jzhua](https://github.com/jzhua)

  - Consume the rest of the stream when scan/query is finished.

- **Improvements**:

  - Support for double precision floating point data type in record bins.
    Requires server version 3.6.0 or later. [CLIENT-599]

  - Support for geospatial data in record bins using GeoJSON format; support
    for querying geospatial indexes using points-within-region and
    region-contains-point filters. Requires server version 3.7.0 or later.
    [CLIENT-594]

  - Tend interval is now configurable via the client policy. Default is 1
    second as before.

  - Only logs tend messages when the number of cluster nodes have changed.

  - Scan and Query termination has been fixed.

## [1.0.10] - 2015-09-22

Major fix release.

- **Fixes**:

  - Fixes `find_node_in_partition_map` logic.

  - Fixes `Node.Refresh` logic.

  - Fixes an issue with dead connections that would cause an infinite loop.

## [1.0.9] - 2015-08-11

Minor fix release.

- **Fixes**:

  - Sends the original key value to the server for all relevant commands, including `operate` and `execute_udf`

## [1.0.8] - 2015-07-23

Minor fix release.

- **Improvements**:

  - Adds 'Filter.to_s'. Thanks to [Ángel M](https://github.com/Angelmmiguel)

- **Fixes**:

  - Fixes an issue in write policy that would use an undefined variable if `:send_key` isn't `nil`.

  - Fixes an issue in cluster.closed logic.

  - Fixes an issue with including the `statement.rb` in the manifest. Thanks to [Ángel M](https://github.com/Angelmmiguel)

## [1.0.7] - 2015-05-15

Minor fixes.

NOTICE: All LDTs on server other than LLIST have been deprecated, and will be removed in the future. As Such, all API regarding those features are considered deprecated and will be removed in tandem.

- **Improvements**:

  - Removed workaround in `BatchGet`. Bins are filtered on server now.

  - Added New Error Codes. Fixes Issues #17 and #18

  - Node validator won't lookup hostsif an IP is passed as a seed to it.

- ** Other Changes **

  - Removed deprecated `ReplaceRoles()` method.

  - Removed deprecated `SetCapacity()` and `GetCapacity()` methods for LDTs.

## [1.0.6] - 2015-04-02

Minor fixes.

- **New Features**:

  - Fixed running a stream query without parameters to the function.

## [1.0.5] - 2015-03-25

Minor improvements.

- **New Features**:

  - Added `:execute_udf_on_query` method to `Aerospike::Client`

## [1.0.4] - 2015-03-24

Hot fix.

- **Fixes**:

  - Close a socket if connection raises an exception to avoid leaking the file descriptor.

## [1.0.3] - 2015-03-24

Minor fixes and improvements.

- **New Features**:

  - Symbols are now accepted as key values. Keep in mind that symbols are converted to string automatically, and type information is lost.

- **Fixes**:

  - Wait for a good connection on `socket.connect_nonblock` to prevent infinite loops on read/write operations.

## [1.0.2] - 2015-03-14

Minor improvements.

- **New Features**:

  - Added `:new_many` method to `Aerospike::Client`

## [1.0.1] - 2015-01-28

Hot fix.

- **Fixes**:

  - Added `bcrypt` to the gem dependencies.

## [1.0.0] - 2015-01-26

Major release. With this release, Ruby client graduates to version 1.

- **Breaking Changes**:

  - All `policy` initialize signatures have changed. Using policies was not documented, so it shouldn't affect most code. It will however, break any code initializing policies.
  - Removed `Record.dups` and `GenerationPolicy::DUPLICATE`

- **New Features**:

  - Added Security Features: Please consult [Security Docs](https://www.aerospike.com/docs/guide/security.html) on Aerospike website.

    - `ClientPolicy.User`, `ClientPolicy.Password`
    - `Client.CreateUser()`, `Client.DropUser()`, `Client.ChangePassword()`
    - `Client.GrantRoles()`, `Client.RevokeRoles()`, `Client.ReplaceRoles()`
    - `Client.QueryUser()`, `Client.QueryUsers`

- **Fixes**:

  - fixed size returned from `BytesValue.write`

## [0.1.6] - 2014-12-28

Minor features added, minor fixes and improvements.

- **New Features**:

  - Added `Policy.consistency_level`
  - Added `WritePolicy.commit_level`

- **Fixes**

  - Fixed setting timeout on connection
  - Fixed exception handling typo for Connection#write

## [0.1.5] - 2014-12-08

Major features added, minor fixes and improvements.

- **New Features**:

  - Added `Client.scan_node`, `Client.scan_all`
  - Added `Client.query`

- **Fixes**

  - Fixed getting back results only for specified bin names.

## [0.1.3] - 2014-10-27

Minor fix.

- **Changes**:

  - Fixed LDT bin and module name packing.

## [0.1.2] - 2014-10-25

Minor fix.

- **Changes**:

  - Fixed String unpacking for single byte strings.

## [0.1.1] - 2014-10-25

Minor fixes.

- **Changes**:

  - Fixed String packing header in Hash and Array.
  - #find on LDTs returns `nil` instad of raising an exception if the item is not found.

## [0.1.0] - 2014-10-14

- Initial Release.
