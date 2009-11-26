require 'drb/drb'

module TinyMapReduce
  class Launcher
    def self.start uri
      DRb.start_service(uri, Worker.new)
      puts DRb.uri
      sleep
    end
  end
end
