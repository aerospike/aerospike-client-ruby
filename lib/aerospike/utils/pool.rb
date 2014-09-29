require 'thread'
require 'timeout'

module Aerospike

  class Pool

    attr_accessor :create_block, :cleanup_block

    def initialize(max_size = 256, &block)
      @create_block = block
      @cleanup_block = nil

      @pool = Queue.new
      @max_size = max_size
    end

    def offer(obj)
      if @pool.length < @max_size
        @pool << obj
      elsif @cleanup_block
        @cleanup_block.call(obj)
      end
    end
    alias_method :<<, :offer

    def poll(create_new=true)
      res = nil
      begin
        res = @pool.pop(true) # non_blocking
        return res
      rescue
        return @create_block.call if @create_block && create_new
      end
    end

    def empty?
      @pool.length == 0
    end

    def length
      @pool.length
    end

  end

end
