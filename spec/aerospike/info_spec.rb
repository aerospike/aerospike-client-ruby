require "spec_helper"
require "benchmark"

describe Aerospike::Info do

  describe "#request" do

    it "should connect and request info from the server" do
      conn = Aerospike::Connection.new("127.0.0.1", 3000)
      Aerospike::Info.request(conn)

      Benchmark.bm do |bm|
        # joining an array of strings
        bm.report do
          1000.times do
            Aerospike::Info.request(conn)
          end
        end
      end

    end
  end

end
