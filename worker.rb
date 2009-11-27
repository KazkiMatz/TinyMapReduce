require 'digest/md5'

module TinyMapReduce
  class Worker
    def initialize
      @im = {}
    end

    def run_map tid, worker_uris, source, m, i, map, combiner = nil
      source = ToEval.setup(source).new(m,i)
      map = ToEval.setup(map)

      to_reduce = []
      while e = source.shift do
        to_reduce << map.call(e)
      end

      if combiner
        to_reduce = run_combine(tid, to_reduce, combiner)
        raise Exceptions::FailureInCombine if to_reduce == 1
      end

      grouped = {}
      to_reduce.each do |item|
        item.each_pair do |key, value|
          i = Digest::MD5.hexdigest(key).unpack("S*")[0] % worker_uris.size
          grouped[i] ||= {}
          grouped[i][key] ||= []
          grouped[i][key] << value
        end
      end

      worker_uris.each_index do |i|
        if grouped[i]
          w = DRbObject.new_with_uri(worker_uris[i])
          w.store_im(tid, grouped[i])
        end
      end

      nil

    rescue => error
      puts "tid-#{tid}:Error(map) #{error.class.to_s} : #{error.message}"
      1
    end

    def run_reduce tid, drain, reduce
      drain = ToEval.setup(drain)
      reduce = ToEval.setup(reduce)

      puts "#{@im[tid].size} keys to reduce" if @im[tid]

      (@im[tid] || {}).each do |key, values|
        puts "tid-#{tid}:key:#{key} - reducing"
        drain.store(tid, key, reduce.call(key, values))
        puts "tid-#{tid}:key:#{key} - finished"
      end

      @im.delete tid

      nil

    rescue => error
      puts "tid-#{tid}:Error(reduce) #{error.class.to_s} : #{error.message}"
      1
    end

    def run_combine tid, to_reduce, combiner
      combiner = ToEval.setup(combiner)

      res = []
      combiner.call(to_reduce).each_pair do |key, value|
        res << {key => value}
      end

      res

    rescue => error
      puts "tid-#{tid}:Error(combine) #{error.class.to_s} : #{error.message}"
      1
    end

    def store_im tid, im
      @im[tid] ||= {}
      im.each_pair do |key, values|
        @im[tid][key] ||= []
        @im[tid][key] += values
      end
      puts "tid-#{tid}:stored #{im.size} keys to IM"

      nil
    end
  end

  class ToEval
    class << self
      def setup code_str
        self.new.to_eval(code_str)
      end
    end

    def to_eval code_str
      eval(code_str)
    end
  end

  module Exceptions
    class FailureInCombine< StandardError; end
  end
end
