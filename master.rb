require 'thread'
require 'drb/drb'
require 'digest/md5'

module TinyMapReduce
  class Master
    attr_accessor :source, :drain, :worker_uris, :tid, :m

    def def_map f
      @map_f = f
    end

    def def_combiner f
      @combiner_f = f
    end

    def def_reduce f
      @reduce_f = f
    end

    def execute
      @m ||= @worker_uris.size
      @tid = Digest::MD5.hexdigest(Time.now.to_f.to_s)

      start_at = Time.now.to_i

      workers = @worker_uris.map{|uri| DRbObject.new_with_uri(uri)}

      puts "Starting Map process..."

      map_try_log = {}
      map_queue = (0..(@m - 1)).to_a
      ts = workers.map{|w|
        Thread.new do
          while i = map_queue.shift
            map_try_log[i] = map_try_log[i] ? map_try_log[i] + 1 : 1
            puts "  assigned source chunk #{i} to #{w.inspect}"
            res = w.run_map(@tid, @worker_uris, @source, m, i, @map_f, @combiner_f)
            if res == 1
              puts "  !!! Map process failed during Source Chunk #{i} on #{w.inspect} !!!"
            else
              puts "  finished mapping source chunk #{i} on #{w.inspect}"
            end
          end
        end
      }
      ts.each {|t| t.join}

      puts "Starting Reduce process..."

      ts = workers.map{|w|
        Thread.new do
          res = w.run_reduce(@tid, @drain, @reduce_f)
          if res == 1
            puts "  !!! Reduce process failed on #{w.inspect} !!!"
          else 
            puts "  finished reducing on #{w.inspect}"
          end
        end
      }
      ts.each {|t| t.join}

      puts "finished MapReduce in #{Time.now.to_i - start_at} sec"

      nil
    end
  end
end
