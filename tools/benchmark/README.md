# Benchmark Tool

Benchmark tool is intended to generate artificial yet customizable load on your
database cluster to help you tweak your connection properties.


## Usage

To see available switches:

    $ ./tools/benchmark/benchmark.rb -u

## How it works

By default, load is generated on keys with values in key range (`-k`
switch). Bin data is static by default.

To generate random bin data, use `-R` switch. To specify the type of bin
data, use `-o` switch. By default it is set to 64 bit integer values.

## Considerations

In our lab tests, we have observed that a concurrency level of 16 can easily
saturate a database node. Increasing concurrency level beyond that doesn't
increase server throughput.

The client is sensitive to timeouts, and they should be chosen carefully.
Connection Timeouts are set using ClientPolicy object, while data operation
timeouts are set in their respective policies. If a connection timeout occurs
during the request, and the number of retries or the operation timeout is not
exhausted, the client will retry the request.

## Examples

To write 10,000,000 keys to the database (static bin data):

    $ ./benchmark.rb -k 10000000

To generate a load consisting 50% reads and 50% updates (static bin data):

    $ ./benchmark.rb -k 10000000 -w RU,50

To generate a load consisting 50% reads and 50% updates, using random bin data:

    $ ./benchmark.rb -k 10000000 -w RU,50 -R

To generate a load consisting 80% reads, using random bin data of strings 50 characters long:

    $ ./benchmark.rb -k 10000000 -w RU,50 -R -o S:50

To generate a load consisting 80% reads, using random bin data of strings 50 characters long, and set a timeout of 10ms:

    $ ./benchmark.rb -k 10000000 -w RU,50 -R -o S:50 -t 50
