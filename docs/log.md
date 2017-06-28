#logger

Various log levels available to log from the Aerospike API.
Default logger outputs to `STDOUT` and is set to `Logger::ERROR` level.

```ruby
  Aerospike.logger.level = Logger::DEBUG
```

You can set the Logger to any type of logger.

## Log levels:

- Logger::ERROR
- Logger::WARN
- Logger::INFO
- Logger::DEBUG
