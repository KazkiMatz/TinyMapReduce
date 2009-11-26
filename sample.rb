module TinyMapReduce
  class Sample

    class Integers
      class << self
        def upto max
          @@max = max
          self
        end
      end

      def initialize m, i
        @m = m
        @i = i
        @at = -m
      end

      def shift
        @at += @m
        v = @at + @i + 1
        if v <= @@max
          v
        else
          nil
        end
      end
    end

    class PNCount < SimpleResource::Base
      include SimpleResource::TtEntityBackend
      class << self
        def store tid, key, value
          create('id' => tid, 'value' => value)
        end
      end
    end

    class << self
      def run_sample uris
        t = TinyMapReduce::Master.new
        t.source = 'TinyMapReduce::Sample::Integers.upto(100000)'
        t.drain = 'TinyMapReduce::Sample::PNCount'
        t.worker_uris = uris

        t.def_map %q!
lambda{|n|
p n
  b = true
  if n > 2
    (2..((n.to_f/2.0).floor)).each{|d|
      next if n % d > 0
      b = false
      break
    }
  elsif n == 1
    b = false
  end
  {"pn_count" => (b ? 1 : 0)}
}
        !

        t.def_combiner %q!
lambda{|arr|
  combined = {}
  arr.each{|hash|
    hash.each_pair {|key, count|
      if combined[key]
        combined[key] += count
      else
        combined[key] = count
      end
    }
  }
  combined
}
        !

        t.def_reduce %q!
lambda{|key,values|
  values.inject(0){|sum, count| sum + count}
}
        !

        t.m = 10000
        t.r = 1

        t.execute

        puts "result: #{PNCount.find(t.tid).value}"
      end
    end
  end
end

