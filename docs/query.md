# Secondary Index Queries

Use Aerospike's fast, high-concurrency queries to perform value-based searches
on secondary indexes. Queries can return a set of records or be processed using
Aerospike UDFs (user-defined functions) before returning to the client.

<a name="manageindex"></a>
## Managing Secondary Indexes

### Creating a Secondary Index

To create a secondary index, invoke
[`Client#create_index`](client.md#createindex). Secondary indexes are created
asynchronously, so the method returns before the secondary index propagates
through the cluster. As an option, the client can wait for the asynchronous
server task to complete.

The following example creates an index *idx_ns_set_bin* on the `bin` bin within
the set `set` and namespace `ns`. The client then waits for the index creation
to complete.

```ruby
task = client.create_index('ns', 'set', 'idx_ns_set_bin', 'bin', :numeric)
task.wait_till_completed
```

Secondary indexes can be created on bins of the following data types:

* `:numeric` - Integers and floats,
* `:string` - Strings,
* `:geo2dsphere` - Geospatial data (points and regions) in GeoJSON format.
  (Require server version 3.7 or later.)

Indexes are also supported on the List and Map data types; for maps, the index
can be created over the map keys or the map values:

* `:list` - Index on list members,
* `:mapkeys` - Index on the map keys,
* `:mapvalues` - Index on the map values.

### Removing a Secondary Index

To remove a secondary index, invoke [`Client#drop_index`](client.md#dropindex):

```ruby
client.drop_index('ns', 'set', 'idx_ns_set_bin')
```

<a name="query"></a>
## Querying Records

### Defining a Query

A query is defined by creating a new instance of the `Aerospike::Statement`
class. The statment specifies the namespace and set on which the query will be
executed. The set may be optional, depending on the secondary index. You can
pass an empty string to omit it. The statement can also optionally specify a
list of bins which will be returned for every record in the result set. (By
default, all bins will be included.)

```ruby
statement = Aerospike::Statement.new('ns', 'set', ['bin1', 'bin2'])
```

### Filters

To limit the records returned by the query, you can add a filter expression. At
the moment, only a single filter expression is supported per query. Filters
expression are created using the `Aerospike::Filter` module. The following
filter expressions are supported:

* `Equal(bin_name, value)` - Filter record bin *bin_name* containing the
  specified value.
* `Range(bin_name, begin, end, col_type=nil)` - Filter record bin *bin_name*
  containing a value in the specified range. If the range filter is applied to
  a collection data type (list or map) then the type of the collection must be
  specified.
* `Contains(bin_name, value, col_type)` - Filter record bin *bin_name*
  containing a collection data type (list or map) which includes the specified
  value.

Example:

```ruby
statement = Aerospike::Statement.new('ns', 'set')
statement.filters << Aerospike::Filter.Range("bin2", 0, 100)
```

#### Geospatial Filters

For record bins containing geospatial data in the form of GeoJSON objects, a
geospatial index can be created, that supports querying the data using
points-within-region and region-contains-point filters:

* `geoWithinGeoJSONRegion(bin_name, region, col_type=nil)` - Matches records
  with a point contained within the specified region.
* `geoWithinRadius(bin_name, lon, lat, radius_meter, col_type=nil)` - Matches
  records with a point contained within the specified radius (in meter) from
  the given lat/lon coordinates.
* `geoContainsGeoJSONPoint(bin_name, point, col_type=nil)` - Matches records
  with a GeoJSON region that contains the specified point.
* `geoContainsPoint(bin_name, lon, lat, col_type=nil)` - Matches records
  with a GeoJSON region that contains the specified lat/lon coordinates.

All geospatial filters can be applied on collections (list and map) as well by
setting the  collection type (`col_type`) on the filter expression.

Geospatial indexes require server version 3.7 or later.

Example:

```ruby
statement = Aerospike::Statement.new('ns', 'set')
coords = Aerospike::GeoJSON.new({ type: "Point", coordinates: [103.91146, 1.30838] })
statement.filters << Aerospike::Filter.geoContainsGeoJSONPoint("loc", coords)
```

### Executing a Query

To execute a query the prepared query statement is passed to the
[`Client#query`](client.md#query) method. The return value is an instance of
`Aerospike::RecordSet` that provides the ability to iterate over the results of the query.

```ruby
statement = Aerospike::Statement.new('ns', 'set')
results = client.query(statment)
results.each do |record|
  # proess the record
end
```

### Predicate Expressions

Predicate Expressions have been introduced in Aerospike Server version 3.12.
They allow additional filtering of records by the server.

A single predicate consists of three parts:

- bin key to check,
- value to compare values to,
- and the predicate to filter the record.

Single expressions can also be chained together by using AND and OR predicates.

Example usage:

```ruby
statement = Aerospike::Statement.new('ns', 'set')
# Return records with bin 'int_bin' greater than 10 and 's_bin' equal to 'test'
statement.predexp = [
  Aerospike::PredExp.integer_bin('int_bin'),
  Aerospike::PredExp.integer_value(10),
  Aerospike::PredExp.integer_greater,
  Aerospike::PredExp.string_bin('s_bin'),
  Aerospike::PredExp.string_value('test'),
  Aerospike::PredExp.string_equal,
  Aerospoke::PredExp.and(2)
]
results = client.query(statment)
results.each do |record|
  # proess the record
end
```

Predicates on bins with Lists and Maps are more complicated. Value has to be saved to a variable, which has to be added to the predicate.

Example:

```ruby
statement = Aerospike::Statement.new('ns', 'set')
# Return records with bin 'list_bin' containing string 'test'
statement.predexp = [
  Aerospike::PredExp.string_value('test'),
  Aerospike::PredExp.string_var('v'),
  Aerospike::PredExp.string_equal,
  Aerospike::PredExp.list_bin('list_bin'),
  Aerospike::PredExp.list_iterate_or('v')
]
results = client.query(statment)
results.each do |record|
  # proess the record
end
```

Values:

```ruby
Aerospike::PredExp.integer_val(10)
Aerospike::PredExp.string_val('example')
Aerospike::PredExp.geojson_val(
  Aerospike::GeoJSON.new(type: 'Point', coordinates: [103.9114,1.3083])
)
```

Variables:
```ruby
Aerospike::PredExp.integer_var('i')
Aerospike::PredExp.string_var('s')
Aerospike::PredExp.geojson_var('geo')
```


Bins:

```ruby
Aerospike::PredExp.integer_bin('age')
Aerospike::PredExp.string_bin('name')
Aerospike::PredExp.geojson_bin('loc')
Aerospike::PredExp.list_bin('list')
Aerospike::PredExp.map_bin('map')
```

Predicates:

```ruby
# and
Aerospike::PredExp.and(2)
# or
Aerospike::PredExp.or(2)
# not
Aerospike::PredExp.not

# Integer predicates
Aerospike::PredExp.integer_equal
Aerospike::PredExp.integer_unequal
Aerospike::PredExp.integer_greater
Aerospike::PredExp.integer_greater_eq
Aerospike::PredExp.integer_less
Aerospike::PredExp.integer_less_eq

# String predicates
Aerospike::PredExp.string_equal
Aerospike::PredExp.string_unequal
Aerospike::PredExp.string_regex(Aerospike::PredExp::RegexFlags::NONE)

# String regex flags
Aerospike::PredExp::RegexFlags::NONE # Regex defaults
Aerospike::PredExp::RegexFlags::EXTENDED # Use POSIX Extended Regular Expression syntax when interpreting regex.
Aerospike::PredExp::RegexFlags::ICASE # Do not differentiate case.
Aerospike::PredExp::RegexFlags::NOSUB # Do not report position of matches.
Aerospike::PredExp::RegexFlags::NEWLINE # Match-any-character operators don't match a newline.

# GeoJSON predicates
Aerospike::PredExp.geojson_contains
Aerospike::PredExp.geojson_within

# List predicates
Aerospike::PredExp.list_iterate_or('x')
Aerospike::PredExp.list_iterate_and('x')

# Map predicates
Aerospike::PredExp.mapkey_iterate_or('x')
Aerospike::PredExp.mapkey_iterate_and('x')
Aerospike::PredExp.mapval_iterate_or('x')
Aerospike::PredExp.mapval_iterate_and('x')

# Record properties
Aerospike::PredExp.last_update
Aerospike::PredExp.void_time
Aerospike::PredExp.record_size
```
