require 'thread'
require 'drb/drb'

module TinyMapReduce
  class Master
    attr_accessor :source, :drain, :worker_uris, :tid, :m, :r
    MAX_MAP_TRY_NUM = 10
    MAX_REDUCE_TRY_NUM = 10

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
      @r ||= @worker_uris.size
      @tid = Digest::MD5.hexdigest(Time.now.to_f.to_s)

      start_at = Time.now.to_i

      workers = @worker_uris.map{|uri| DRbObject.new_with_uri(uri)}

      keys_to_reduce = []

      map_try_log = {}
      map_queue = (0..(@m - 1)).to_a
      ts = workers.map{|w|
        Thread.new do
          while i = map_queue.shift
            map_try_log[i] = map_try_log[i] ? map_try_log[i] + 1 : 1
            puts "assigned source chunk #{i} to #{w.inspect}"
            res = w.run_map(@tid, @source, m, i, @map_f, @combiner_f)
            if res.is_a? Array
              keys_to_reduce += res
              keys_to_reduce = keys_to_reduce.flatten.uniq
              puts "finished mapping source chunk #{i} by #{w.inspect}"
            elsif res == 1
              #puts "! A Job Halted during processing source chunk #{i} by #{w.inspect} !"
              #if map_try_log[i] < MAX_MAP_TRY_NUM
              #  map_queue << i
              #else
                puts "!!! Source Chunk #{i} Cancelled !!!"
              #end
            end
          end
        end
      }
      ts.each {|t| t.join}

      puts "#{keys_to_reduce.size} key(s) to reduce"

      reduce_try_log = {}
      reduce_queue = []
      steps = (keys_to_reduce.size.to_f / @r.to_f).ceil
      p = 0
      while p < keys_to_reduce.size
        reduce_queue << keys_to_reduce[p, steps]
        p += steps
      end
      total_num = reduce_queue.size

      ts = workers.map{|w|
        Thread.new do
          while keys = reduce_queue.shift
            reduce_try_log[keys] = reduce_try_log[keys] ? reduce_try_log[keys] + 1 : 1
            puts "assigned im chunk #{keys.inspect} to #{w.inspect}"
            res = w.run_reduce(@tid, keys, @drain, @reduce_f)
            if res == nil
              puts "finished reducing im chunk #{keys.inspect} by #{w.inspect}"
            elsif res == 1
              #puts "! A Job Halted during processing im chunk #{keys.inspect} by #{w.inspect} !"
              #if reduce_try_log[keys] < MAX_REDUCE_TRY_NUM
              #  reduce_queue << keys
              #else
                puts "!!! im chunk #{keys.inspect} Cancelled !!!"
              #end
            end
          end
        end
      }
      ts.each {|t| t.join}

      puts "finished MapReduce in #{Time.now.to_i - start_at} sec"

      nil
    end
  end
end
