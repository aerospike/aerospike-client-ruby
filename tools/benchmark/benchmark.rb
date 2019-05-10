#!/usr/bin/env ruby

require 'time'
require 'thread'
require 'optparse'
require 'rubygems'

require 'aerospike'

include Aerospike

@options = {
  :port => 3000,
  :namespace => 'test',
  :set => 'benchmark',
  :key_count => 100000,
  :bin_def => 'I',
  :concurrency => 4,
  :workload_def => 'I:100',
  :throughput => 0,
  :timeout => 0,
  :max_retries => 2,
  :conn_queue_size => 64,
  :rand_bin_data => false,
  :debug_mode => false,
  :user => '',
  :password => '',
}

@mutex = Mutex.new

@opt_parser = OptionParser.new do |opts|
  opts.banner = "Usage: benchmark [@options]"

  opts.on("-h", "--host HOST", "Aerospike server seed hostnames or IP addresses") do |v|
    @options[:host] = v
  end

  opts.on("-p", "--port PORT", "Aerospike server seed hostname or IP address port number.") do |v|
    @options[:port] = v.to_i
  end

  opts.on("-U", "--user USER", "Aerospike user name") do |v|
    @options[:user] = v
  end

  opts.on("-P", "--password PASSWORD", "Aerospike user password") do |v|
    @options[:password] = v
  end

  opts.on("-n", "--namespace NAMESPACE", "Aerospike namespace.") do |v|
    @options[:namespace] = v
  end

  opts.on("-s", "--set SET", "Aerospike set name.") do |v|
    @options[:set] = v
  end

  opts.on("-k", "--keys KEYS", "Key/record count or key/record range.") do |v|
    @options[:key_count] = v.to_i
  end

  opts.on("-o", "--object OBJECT", "Bin object specification.\n\t\t\t\t\tI\t: Read/write integer bin.\n\t\t\t\t\tB:200\t: Read/write byte array bin of length 200.\n\t\t\t\t\tS:50\t: Read/write string bin of length 50.") do |v|
    @options[:bin_def] = v
  end

  opts.on("-c", "--concurrency COUNT", "Number of threads to generate load.") do |v|
    @options[:concurrency] = v.to_i
  end

  opts.on("-w", "--workload TYPE", "Desired workload.\n\t\t\t\t\tI:60\t: Linear 'insert' workload initializing 60% of the keys.\n\t\t\t\t\tRU:80\t: Random read/update workload with 80% reads and 20% writes.") do |v|
    @options[:workload_def] = v
  end

  opts.on("-g", "--throttle VALUE", "Throttle transactions per second to a maximum value.\n\t\t\t\t\tIf tps is zero, do not throttle throughput.\n\t\t\t\t\tUsed in read/write mode only.") do |v|
    @options[:throughput] = v.to_i
  end

  opts.on("-t", "--timeout MILISECONDS", "Read/Write timeout in milliseconds.") do |v|
    @options[:timeout] = v.to_i / 1000.to_f
  end

  opts.on("-m", "--max-retries COUNT", "Maximum number of retries before aborting the current transaction.") do |v|
    @options[:max_retries] = v.to_i
  end

  opts.on("-q", "--queue-size SIZE", "Maximum number of connections to pool.") do |v|
    @options[:conn_queue_size] = v.to_i
  end

  opts.on("-R", "--random-bins", "Use dynamically generated random bin values instead of default static fixed bin values.") do |v|
    @options[:rand_bin_data] = v
  end

  opts.on("-d", "--debug", "Run benchmarks in debug mode.") do |v|
    @options[:debug_mode] = v
  end

  opts.on("-u", "--usage", "Show usage information.") do |v|
    puts opts
    exit
  end
end # opt_parser


def workloadToString
  case @workloadType
  when 'RU'
    "Read #{@workloadPercent}%, Write #{100-@workloadPercent}%"
  else
    "Initialize #{@workloadPercent}% of records"
  end
end

def throughputToString
  if @options[:throughput] <= 0
    "unlimited"
  else
    "#{@options[:throughput]}"
  end
end

def printBenchmarkParams
  puts("hosts:\t\t#{@options[:host]}")
  puts("port:\t\t#{@options[:port]}")
  puts("namespace:\t#{@options[:namespace]}")
  puts("set:\t\t#{@options[:set]}")
  puts("keys/records:\t#{@options[:key_count]}")
  puts("object spec:\t#{@binDataType}, size: #{@binDataSize}")
  puts("random bins:\t#{@options[:rand_bin_data]}")
  puts("workload:\t#{workloadToString}")
  puts("concurrency:\t#{@options[:concurrency]}")
  puts("max throughput:\t#{throughputToString}")
  puts("timeout:\t#{@options[:timeout] > 0 ? (@options[:timeout] * 1000).to_i : '-'} ms")
  puts("max retries:\t#{@options[:max_retries]}")
  puts("debug:\t\t#{@options[:debug_mode]}")
  puts
end

# parses an string of (key:value) type
def parseValuedParam(param)
  re = /(?<type>\w+)([:,](?<value>\d+))?/
  values = re.match(param)
  if values
    [values[:type], values[:value].to_i]
  else
    [nil, nil]
  end
end

# reads input flags and interprets the complex ones
def readFlags
  @opt_parser.parse!

  Aerospike.logger.level = Logger::ERROR
  if @options[:debug_mode]
    Aerospike.logger.level = Logger::INFO
  end

  @binDataType, binDataSz = parseValuedParam(@options[:bin_def])
  if binDataSz
    @binDataSize = @options[:binDataSz]
  else
    case @binDataType
    when 'B'
      @binDataSize = 200
    when 'S'
      @binDataSize = 50
    end
  end

  @workloadType, workloadPct = parseValuedParam(@options[:workload_def])
  if workloadPct
    @workloadPercent = workloadPct.to_i
  else
    case @workloadType
    when 'I'
      @workloadPercent = 100
    when 'RU'
      @workloadPercent = 50
    end
  end
end

# new random bin generator based on benchmark specs
def getBin
  case @binDataType
  when 'B'
    bin = Bin.new('information', BytesValue.new(randString(@binDataSize)))
  when 'S'
    bin = Bin.new('information', StringValue.new(randString(@binDataSize)))
  else
    bin = Bin.new('information', IntegerValue.new(2**63))
  end

  bin
end

# generates a random strings of specified length
@RAND_CHARS = ('a'..'z').to_a.concat(('A'..'Z').to_a).concat(('0'..'9').to_a)
def randString(size)
  @RAND_CHARS.shuffle[0,size].join
end


@totalWCount, @totalRCount = 0, 0
@totalWErrCount, @totalRErrCount = 0, 0
@totalTOCount, @totalWTOCount, @totalRTOCount = 0, 0, 0

@terminate = false
Signal.trap("INT") do
  @terminate = true
end

def run_bench(client, ident, times)
  writepolicy = WritePolicy.new
  client.default_write_policy.timeout = @options[:timeout]
  client.default_write_policy.max_retries = @options[:max_retries]

  client.default_write_policy = writepolicy

  defaultBin = getBin

  t = Time.now

  w_count, r_count = 0, 0
  write_err, read_err = 0, 0
  write_to_err, read_to_err  = 0, 0

  bin = defaultBin
  namespace = @options[:namespace]
  set = @options[:set]
  randbins = @options[:rand_bin_data]

  iters = 1
  while (@workloadType == 'RU' || iters <= times) && !@terminate
    # if randomBin data has been requested
    bin = getBin if randbins
    key = Key.new(namespace, set, ident*times+(iters%times))
    if (@workloadType == 'I') || (rand(100) >= @workloadPercent)
      begin
        client.put(key, bin)
        w_count+=1
      rescue Exception => err
        if err.is_a?(Aerospike::Exceptions::Timeout)
          write_to_err+=1
        else
          write_err +=1
        end
      end
    else
      begin
        client.get(key, [bin.name])
        r_count+=1
      rescue Exception => err
        if err.is_a?(Aerospike::Exceptions::Timeout)
          read_to_err +=1
        else
          read_err +=1
        end
      end
    end

    if Time.now - t >= 0.3
      @mutex.synchronize do
        @totalWCount += w_count
        @totalRCount += r_count
        @totalWErrCount += write_err
        @totalRErrCount += read_err
        @totalWTOCount += write_to_err
        @totalRTOCount += read_to_err
      end

      w_count, r_count = 0, 0
      write_err, read_err = 0, 0
      write_to_err, read_to_err = 0, 0
      t = Time.now
    end
    iters += 1
  end
end

def log_stats(timeElapsed:, reads: 0, readTimeouts: 0, readErrors: 0, writes: 0, writeTimeouts: 0, writeErrors: 0)
  readTPS = reads / timeElapsed
  writeTPS = writes / timeElapsed
  total = reads + writes
  totalTPS = total / timeElapsed
  totalTimeouts = readTimeouts + writeTimeouts
  totalErrors = readErrors = writeErrors

  if @workloadType == 'RU'
    str = "write(tps=#{writeTPS.round} timeouts=#{writeTimeouts} errors=#{writeErrors})"
    str << " read(tps=#{readTPS.round} timeouts=#{readTimeouts} errors=#{readErrors})"
    str << " total(tps=#{totalTPS.round} timeouts=#{totalTimeouts} errors=#{totalErrors}, count=#{total})"
    @logger.info str
  else
    @logger.info "write(tps=#{writeTPS.round} timeouts=#{writeTimeouts} errors=#{writeErrors} totalCount=#{writes})"
  end
end

def log_final(timeElapsed)
  @logger.info "Totals: (run time #{timeElapsed} sec)"
  log_stats(timeElapsed: timeElapsed,
           reads: @totalRCount,
           readTimeouts: @totalRTOCount,
           readErrors: @totalRErrCount,
           writes: @totalWCount,
           writeTimeouts: @totalWTOCount,
           writeErrors: @totalWErrCount
          )
end

def reporter
  last_totalWCount = 0
  last_totalRCount = 0
  last_totalWErrCount = 0
  last_totalRErrCount = 0
  last_totalWTOCount = 0
  last_totalRTOCount = 0

  t = Time.now
  while true
    timeElapsed = Time.now - t
    if timeElapsed >= 1
      @mutex.synchronize do
        log_stats(timeElapsed: timeElapsed,
                 reads: @totalRCount - last_totalRCount,
                 readTimeouts: @totalRTOCount - last_totalRTOCount,
                 readErrors: @totalRErrCount - last_totalRErrCount,
                 writes: @totalWCount - last_totalWCount,
                 writeTimeouts: @totalWTOCount - last_totalWTOCount,
                 writeErrors: @totalWErrCount - last_totalWErrCount
                )

        last_totalWCount = @totalWCount
        last_totalRCount = @totalRCount
        last_totalWErrCount = @totalWErrCount
        last_totalRErrCount = @totalRErrCount
        last_totalWTOCount = @totalWTOCount
        last_totalRTOCount = @totalRTOCount
      end

      t = Time.now
    end

    sleep(0.1)
  end
end

@logger = Logger.new(STDOUT)
@logger.level = Logger::INFO

readFlags
printBenchmarkParams

begin
  host = Host.new(@options[:host], @options[:port])
  policy = { user: @options[:user], password: @options[:password] }
  client = @options[:host] ? Client.new(host, policy: policy) : Client.new(policy: policy)
rescue => e
  abort(e.to_s)
end

r_thread = Thread.new do
  reporter
end

start = Time.now
threads = []
for i in (1..@options[:concurrency]) do
    threads << Thread.new {run_bench(client, i - 1, @options[:key_count] / @options[:concurrency]) }
end
threads.each(&:join)
@total_time = Time.now - start

r_thread.kill
log_final(@total_time)
