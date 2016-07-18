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
statement.filters << Aerospike::Filter.Range("bin2", 0, 100))
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
