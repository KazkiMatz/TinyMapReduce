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

    class MCount < SimpleResource::Base
      include SimpleResource::TtEntityBackend
      class << self
        def store tid, key, value
          create('id' => "#{tid}/#{key}", 'value' => value)
        end
      end
    end

    class << self
      def run_sample uris
        t = TinyMapReduce::Master.new
        t.source = 'TinyMapReduce::Sample::Integers.upto(1000000)'
        t.drain = 'TinyMapReduce::Sample::MCount'
        t.worker_uris = uris

        t.map = %q!
lambda{|n|
  res = {}
  res['m_of_2'] = 1 if n%2 == 0
  res['m_of_3'] = 1 if n%3 == 0
  res
}
        !

        t.combiner = %q!
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

        t.reduce = %q!
lambda{|key,values|
  values.inject(0){|sum, count| sum + count}
}
        !

        t.m = 100

        t.execute

        puts "result - m_of_2: #{MCount.find("#{t.tid}/m_of_2").value}"
        puts "result - m_of_3: #{MCount.find("#{t.tid}/m_of_3").value}"
      end
    end
  end
end

