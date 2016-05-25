# Operations

The `Client#operate` method provides the ability to execute multiple operations
on a single record in the database as a single atomic transaction. Operations
can read, write or update record bins and read and write operations can be
mixed in a single `operate` command. Operations are executed in the order in
which they are specified. E.g. an increment operation on an integer bin,
followed by a read operation on the same bin, will return the new value
post-increment.

The results are returned as a hash map with the bin names as keys and the
result of the last operation on that bin as the value. I.e. if multiple
operations on the same bin return a value for that bin, the result of the last
operation will be returned. Note that many write operations return values as
well, e.g. a list append operation returns the new size of the list after the
append operation.

Operations are grouped by the type of bins that they operate on. Operations are
instantiated via one of the following three classes:

- `Aerospike::Operation` - operations on strings, integers and byte arrays,
- `Aerospike::CDT::ListOperation` - operations on list values (see
  [List CDT](http://www.aerospike.com/docs/guide/cdt-list.html), requires
  Aerospike Server version 3.8 or later),
- `Aerospike::CDT::MapOperation` - operations on map values (see
  [Map CDT](http://www.aerospike.com/docs/guide/cdt-map.html), requires
  Aerospike Server version 3.9 or later).

<a name="operations"></a>
## Operations on scalar values (Strings, Integers, etc.)

<a name="get"></a>
### Aerospike::Operation#get(bin_name)

Returns the value of a bin. If called without a bin name, the operation returns
the entire record.

Parameters:

- `bin_name`       - [optional] The name of the bin.

<a name="get_header"></a>
### Aerospike::Operation#get_header()

Returns the record's meta data (generation, ttl, etc.).

<a name="put"></a>
### Aerospike::Operation#put(bin)

Replaces a bin with a new value.

Parameters:

- `bin`       - A [record bin](datamodel.md#bin).

<a name="append"></a>
### Aerospike::Operation#append(bin)

Append the value to the bin. The bin must contain either String or a Byte
Array, and the value must be of the same type.

Parameters:

- `bin`       - A [record bin](datamodel.md#bin).

<a name="prepend"></a>
### Aerospike::Operation#prepend(bin)

Prepend the value to the bin. The bin must contain either String or a Byte
Array, and the value must be of the same type.

Parameters:

- `bin`       - A [record bin](datamodel.md#bin).

<a name="add"></a>
### Aerospike::Operation#add(bin)

Increment the value of the bin by the given value. The bin must contain either
an Integer or a Double, and the value must be of the same type.

Parameters:

- `bin`       - A [record bin](datamodel.md#bin).

<a name="touch"></a>
### Aerospike::Operation#touch()

Update the TTL of a record.

<a name="list-operations"></a>
## Operations on List bins

<a name="list-append"></a>
### Aerospike::CDT::ListOperation#append(bin_name)

Appends an element to the end of a list.
Server returns list size.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a list value.
- `value`          - Value to append to list.


<a name="list-insert"></a>
### Aerospike::CDT::ListOperation#insert(bin_name, index, *values)

Inserts an element at the specified index.
Server returns list size.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a list value.
- `index`          - List index at which the new element should be inserted.
- `values`         - One or more values to append to list.

<a name="list-pop"></a>
### Aerospike::CDT::ListOperation#pop(bin_name, index)

Removes and returns the list element at the specified index.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a list value.
- `index`          - List index of the element to be removed.

<a name="list-pop_range"></a>
### Aerospike::CDT::ListOperation#pop_range(bin_name, index, count)

Returns "count" items starting at specified index and removes them from the
list. If "count" is not specified, the server returns items starting at the
specified index to the end of the list and removes those items from the list
bin.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a list value.
- `index`          - List index of the element to be removed.
- `count`          - [optional] Number of elements in the range; if not
                     specified, the range extends to the end of the list.

<a name="list-remove"></a>
### Aerospike::CDT::ListOperation#remove(bin_name, index)

Removes item at specified index from list bin.
Server returns number of items removed.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a list value.
- `index`          - List index of the element to be removed.

<a name="list-remove_range"></a>
### Aerospike::CDT::ListOperation#remove_range(bin_name, index, count)

Removes "count" items at specified index from list bin. If "count" is not
specified, the server removes all items starting at the specified index to the
end of the list.
Server returns number of items removed.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a list value.
- `index`          - List index of the element to be removed.
- `count`          - [optional] Number of elements in the range; if not
                     specified, the range extends to the end of the list.

<a name="list-set"></a>
### Aerospike::CDT::ListOperation#set(bin_name, index, value)

Sets item value at specified index in list bin.
Server does not return a result by default.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a list value.
- `index`          - List index of the element to be replaced.
- `value`          - The new value to be assigned to the list element.

<a name="list-trim"></a>
### Aerospike::CDT::ListOperation#trim(bin_name, index, count)

Removes items in list bin that do not fall into range specified by index and
count. If count is not specified, server will keep all items starting at the
specified index to the end of the list and remove the rest.
Server returns number of items removed.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a list value.
- `index`          - List index of the element to be replaced.
- `count`          - [optional] Number of elements in the range; if not
                     specified, the range extends to the end of the list.

<a name="list-clear"></a>
### Aerospike::CDT::ListOperation#clear(bin_name)

Removes all items in the list bin.
Server does not return a result by default.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a list value.

<a name="list-size"></a>
### Aerospike::CDT::ListOperation#size(bin_name)

Returns size of list.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a list value.

<a name="list-get"></a>
### Aerospike::CDT::ListOperation#get(bin_name, index)

Returns the item at the specified index in the list bin.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a list value.
- `index`          - List index of the element.

<a name="list-get_range"></a>
### Aerospike::CDT::ListOperation#get_range(bin_name, index, count)

Returns "count" items starting at the specified index in the list bin. If
"count" is not specified, the server returns all items starting at the
specified index to the end of the list.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a list value.
- `index`          - List index of the element.
- `count`          - [optional] Number of elements in the range; if not
                     specified, the range extends to the end of the list.

<a name="map-operations"></a>
## Operations on Map bins

<a name="map-set_policy"></a>
### Aerospike::CDT::MapOperation#set_policy(bin_name, policy)

Sets map policy attributes. Server returns null.

The required map policy attributes can be changed after the map is created.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `policy`         - `MapPolicy` instance.

<a name="map-put"></a>
### Aerospike::CDT::MapOperation#put(bin_name, key, value, policy:)

Writes key/value item to map bin and returns map size.

The map policy dictates the type of map to create when it does not exist.
The map policy also specifies the mode used when writing items to the map.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `key`            - Key of the map entry to create/update.
- `value`          - New value.
- `policy`         - [optional] `MapPolicy` instance.

<a name="map-put_items"></a>
### Aerospike::CDT::MapOperation#put_items(bin_name, values, policy:)

Writes each map item to map bin and returns map size.

The map policy dictates the type of map to create when it does not exist.
The map policy also specifies the mode used when writing items to the map.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `values`         - Hash of key/value map entries.
- `policy`         - [optional] `MapPolicy` instance.

<a name="map-increment"></a>
### Aerospike::CDT::MapOperation#increment(bin_name, key, incr, policy:)

Increments values by incr for all items identified by key and returns final
result. Valid only for numbers.

The map policy dictates the type of map to create when it does not exist.
The map policy also specifies the mode used when writing items to the map.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `key`            - Map key.
- `incr`           - Value to increment the map entry by.
- `policy`         - [optional] `MapPolicy` instance.

<a name="map-decrement"></a>
### Aerospike::CDT::MapOperation#decrement(bin_name, key, decr, policy:)

Decrements values by decr for all items identified by key and returns final
result. Valid only for numbers.

The map policy dictates the type of map to create when it does not exist.
The map policy also specifies the mode used when writing items to the map.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `key`            - Map key.
- `decr`           - Value to decrement the map entry by.

<a name="map-clear"></a>
### Aerospike::CDT::MapOperation#clear(bin_name)

Removes all items in map. Server returns null.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.

<a name="map-remove_keys"></a>
### Aerospike::CDT::MapOperation#remove_keys(bin_name, *keys, return_type:)

Removes map item identified by key and returns removed data specified by return_type.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `*keys`          - One or more map keys to remove.
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-remove_key_range"></a>
### Aerospike::CDT::MapOperation#remove_key_range(bin_name, key_begin, key_end, return_type:)

Removes map items identified by key range (key_begin inclusive, key_end
exclusive). If key_begin is null, the range is less than key_end. If key_end is
null, the range is greater than equal to key_begin.

Server returns removed data specified by return_type.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `key_begin`      - Start of the key range (inclusive).
- `key_end`        - End of the key range (exclusive).
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-remove_values"></a>
### Aerospike::CDT::MapOperation#remove_values(bin_name, *values, return_type:)

Removes map items identified by value and returns removed data specified by return_type.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `values`         - List of values to be removed from the map bin.
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-remove_value_range"></a>
### Aerospike::CDT::MapOperation#remove_value_range(bin_name, value_begin, value_end, return_type:)

Removes map items identified by value range (value_begin inclusive, value_end exclusive).
If value_begin is null, the range is less than value_end.
If value_end is null, the range is greater than equal to value_begin.

Server returns removed data specified by return_type.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `value_begin`    - Start of the value range (inclusive).
- `value_end`      - End of the value range (exclusive).
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-remove_value_range"></a>
### Aerospike::CDT::MapOperation#remove_value_range(bin_name, value_begin, value_end, return_type:)

Removes map items identified by value range (value_begin inclusive, value_end exclusive).
If value_begin is null, the range is less than value_end.
If value_end is null, the range is greater than equal to value_begin.

Server returns removed data specified by return_type.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `value_begin`    - Start of the value range (inclusive).
- `value_end`      - End of the value range (exclusive).
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-remove_index"></a>
### Aerospike::CDT::MapOperation#remove_index(bin_name, index, return_type:)

Removes map item identified by index and returns removed data specified by return_type.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `index`          - Index of the map entry to be removed.
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-remove_index_range"></a>
### Aerospike::CDT::MapOperation#remove_index_range(bin_name, index, count, return_type:)

Removes "count" map items starting at specified index and returns removed data
specified by return_type. If "count" is not specified, the server selects map
items starting at specified index to the end of map.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `index`          - Index of the map entry to be removed.
- `count`          - [optional] Number of items to remove.
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-remove_by_rank"></a>
### Aerospike::CDT::MapOperation#remove_by_rank(bin_name, rank, return_type:)

Removes map item identified by rank and returns removed data specified by return_type.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `rank`           - Rank of the item(s) to be removed.
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-remove_by_rank_range"></a>
### Aerospike::CDT::MapOperation#remove_by_rank_range(bin_name, rank, count, return_type:)

Selects "count" map items starting at specified rank and returns selected data
specified by return_type. If "count" is not specified, server removes map items
starting at specified rank to the last ranked.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `rank`           - Rank of the item(s) to be removed.
- `count`          - [optional] Number of items to remove.
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-size"></a>
### Aerospike::CDT::MapOperation#size(bin_name)

Returns size of map.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.

<a name="map-get_key"></a>
### Aerospike::CDT::MapOperation#get_key(bin_name, key, return_type:)

Selects map item identified by key and returns selected data specified by return_type.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `key`            - Key of the map item.
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-get_key_range"></a>
### Aerospike::CDT::MapOperation#get_key_range(bin_name, key_begin, key_end, return_type:)

Selects map items identified by key range (key_begin inclusive, key_end
exclusive). If key_begin is null, the range is less than key_end. If key_end is
null, the range is greater than equal to key_begin.

Server returns selected data specified by return_type.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `key_begin`      - Start key of the range (inclusive).
- `key_end`        - End key of the range (exclusive).
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-get_value"></a>
### Aerospike::CDT::MapOperation#get_value(bin_name, value, return_type:)

Selects map items identified by value and returns selected data specified by
return_type.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `value`          - Value of map item(s) to return.
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-get_value_range"></a>
### Aerospike::CDT::MapOperation#get_value_range(bin_name, value_begin, value_end, return_type:)

Selects map items identified by value range (value_begin inclusive, value_end
exclusive) If value_begin is null, the range is less than value_end. If
value_end is null, the range is greater than equal to value_begin.

Server returns selected data specified by return_type.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `value_begin`    - Start value of the range.
- `value_end` `    - End value of the range.
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-get_index"></a>
### Aerospike::CDT::MapOperation#get_index(bin_name, index, return_type:)

Selects map items identified by index and returns selected data specified by
return_type.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `index`          - Index of map item to return.
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-get_index_range"></a>
### Aerospike::CDT::MapOperation#get_index_range(bin_name, index, count, return_type:)

Server selects "count" map items starting at specified index and returns
selected data specified by return_type.  If "count" is not specified, server
selects map items starting at specified index to the end of map.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map index.
- `index`          - Start index of the range.
- `count` `        - [optional] Number of elements to return.
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-get_rank"></a>
### Aerospike::CDT::MapOperation#get_rank(bin_name, rank, return_type:)

Selects map items identified by rank and returns selected data specified by
return_type.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map value.
- `rank`           - Rank of map item to return.
- `return_type`    - [optional] Type of data to return. Default is none.

<a name="map-get_rank_range"></a>
### Aerospike::CDT::MapOperation#get_rank_range(bin_name, rank, count, return_type:)

Server selects "count" map items starting at specified rank and returns
selected data specified by return_type.  If "count" is not specified, server
selects map items starting at specified rank to the end of map.

Parameters:

- `bin_name`       - Name of the bin; the bin must contain a map rank.
- `rank`           - Start rank of the range.
- `count` `        - [optional] Number of elements to return.
- `return_type`    - [optional] Type of data to return. Default is none.
