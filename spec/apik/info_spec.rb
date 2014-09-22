require "spec_helper"
require "benchmark"

describe Apik::Info do

  describe "#request" do

    it "should connect and request info from the server" do
      conn = Apik::Connection.new("127.0.0.1", 3000)
      Apik::Info.request(conn)

      Benchmark.bm do |bm|
        # joining an array of strings
        bm.report do
          1000.times do
            Apik::Info.request(conn)
          end
        end
      end

    end
  end

end
