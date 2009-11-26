require 'drb/drb'

module TinyMapReduce
  class Launcher
    def self.start uri
      $tt_conn = PureMemcached.new(TT_HOST[0], {:timeout => 30})

      DRb.start_service(uri, Worker.new)
      puts DRb.uri
      sleep
    end
  end
end
