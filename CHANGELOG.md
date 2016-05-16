Head
===================

* **Improvements**
  * Add 2.3.1 to supported Ruby versions on Travis-CI.

1.0.12 / 2016-02-11
===================

* **Fixes**:

  * Fixed syntax error in Client when raising exception for invalid bin key;
    thanks to [Ole Riesenberg](https://github.com/oleriesenberg) for the fix.
    [aerospike/aerospike-client-ruby#31]
  * Use UTF-8 as default encoding when writing/reading Strings from record
    bins; thanks to [fs-wu](https://github.com/fs-wu) for finding the issue and
    reporting it. [aerospike/aerospike-client-ruby#33]

## December 4 2015 (1.0.11)

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

## September 22 2015 (1.0.10)

  Major fix release.

  * **Fixes**:

    * Fixes `find_node_in_partition_map` logic.

    * Fixes `Node.Refresh` logic.

    * Fixes an issue with dead connections that would cause an infinite loop.

## Augest 11 2015 (1.0.9)

  Minor fix release.

  * **Fixes**:

    * Sends the original key value to the server for all relevant commands, including `operate` and `execute_udf`

## July 23 2015 (1.0.8)

  Minor fix release.

  * **Improvements**:

    * Adds 'Filter.to_s'. Thanks to [Ángel M](https://github.com/Angelmmiguel)

  * **Fixes**:

    * Fixes an issue in write policy that would use an undefined variable if `:send_key` isn't `nil`.

    * Fixes an issue in cluster.closed logic.

    * Fixes an issue with including the `statement.rb` in the manifest. Thanks to [Ángel M](https://github.com/Angelmmiguel)

## May 15 2015 (1.0.7)

  Minor fixes.

  NOTICE: All LDTs on server other than LLIST have been deprecated, and will be removed in the future. As Such, all API regarding those features are considered deprecated and will be removed in tandem.

  * **Improvements**:

    * Removed workaround in `BatchGet`. Bins are filtered on server now.

    * Added New Error Codes. Fixes Issues #17 and #18

    * Node validator won't lookup hostsif an IP is passed as a seed to it.

  * ** Other Changes **

    * Removed deprecated `ReplaceRoles()` method.

    * Removed deprecated `SetCapacity()` and `GetCapacity()` methods for LDTs.

## April 2 2015 (1.0.6)

  Minor fixes.

  * **New Features**:

    * Fixed running a stream query without parameters to the function.

## March 25 2015 (1.0.5)

  Minor improvements.

  * **New Features**:

    * Added `:execute_udf_on_query` method to `Aerospike::Client`

## March 24 2015 (1.0.4)

  Hot fix.

  * **Fixes**:

    * Close a socket if connection raises an exception to avoid leaking the file descriptor.

## March 24 2015 (1.0.3)

  Minor fixes and improvements.

  * **New Features**:

    * Symbols are now accepted as key values. Keep in mind that symbols are converted to string automatically, and type information is lost.

  * **Fixes**:

    * Wait for a good connection on `socket.connect_nonblock` to prevent infinite loops on read/write operations.


## March 14 2015 (1.0.2)

  Minor improvements.

  * **New Features**:

    * Added `:new_many` method to `Aerospike::Client`

## Jan 28 2015 (1.0.1)

  Hot fix.

  * **Fixes**:

    * Added `bcrypt` to the gem dependencies.

## Jan 26 2015 (1.0.0)

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

## Dec 28 2014 (0.1.6)

  Minor features added, minor fixes and improvements.

  * **New Features**:

    * Added `Policy.consistency_level`
    * Added `WritePolicy.commit_level`

  * **Fixes**

    * Fixed setting timeout on connection
    * Fixed exception handling typo for Connection#write

## Dec 8 2014 (0.1.5)

  Major features added, minor fixes and improvements.

  * **New Features**:

    * Added `Client.scan_node`, `Client.scan_all`
    * Added `Client.query`

  * **Fixes**

    * Fixed getting back results only for specified bin names.

## Oct 27 2014 (0.1.3)

  Minor fix.

  * **Changes**:

    * Fixed LDT bin and module name packing.

## Oct 25 2014 (0.1.2)

  Minor fix.

  * **Changes**:

    * Fixed String unpacking for single byte strings.

## Oct 25 2014 (0.1.1)

  Minor fixes.

  * **Changes**:

    * Fixed String packing header in Hash and Array.
    * #find on LDTs returns `nil` instad of raising an exception if the item is not found.

## Oct 14 2014 (0.1.0)

  * Initial Release.
