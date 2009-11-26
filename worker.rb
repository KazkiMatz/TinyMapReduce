require 'thread'

module TinyMapReduce
  class Worker
    def initialize
      @map_mutex = Mutex.new
      @reduce_mutex = Mutex.new
    end

    def gen_map_env source_klass, m, i, map, combiner
      @source = eval(source_klass).new(m, i)
      @map = eval(map)
      @combiner = eval(combiner) if combiner
    end

    def gen_reduce_env drain_klass, reduce
      @drain = eval(drain_klass)
      @reduce = eval(reduce)
    end

    def run_map tid, source_klass, m, i, map, combiner
      @map_mutex.lock

      gen_map_env source_klass, m, i, map, combiner

      to_combine = []
      while e = @source.shift do
        to_combine << @map.call(e)
      end

      keys_to_reduce = []
      if @combiner
        combined_hash = @combiner.call(to_combine)

        combined_hash.each_pair do |key, value|
          ImKey.find_or_create_with_lock("#{tid}:#{key}", {}) do |im_key|
            im_key.add! value
          end
          keys_to_reduce << key unless keys_to_reduce.include?(key)
        end
      else
        while e = to_combine.shift do
          e.each_pair do |key, value|
            ImKey.find_or_create_with_lock("#{tid}:#{key}", {}) do |im_key|
              im_key.add! value
            end
            keys_to_reduce << key unless keys_to_reduce.include?(key)
          end
        end
      end

      keys_to_reduce

    rescue => error
      puts "(map) #{error.class.to_s} : #{error.message}"
      1
    ensure
      @map_mutex.unlock
    end

    def run_reduce tid, keys_to_reduce, drain_klass, reduce
      @reduce_mutex.lock

      gen_reduce_env drain_klass, reduce

      keys_to_reduce.each do |key|
        im_key = ImKey.find("#{tid}:#{key}")
        @drain.store(tid, key, @reduce.call(key, im_key.values))
      end

      keys_to_reduce.each do |key|
        ImKey.find("#{tid}:#{key}").destroy!
      end

      nil
    rescue => error
      puts "(reduce) #{error.class.to_s} : #{error.message}"
      1
    ensure
      @reduce_mutex.unlock
    end
  end

  class ImKey < SimpleResource::Base
    include SimpleResource::TtEntityBackend

    class << self
      def create attributes
        l = ImList.create('list' => [])
        attributes['head'] = l.id
        attributes['tail'] = l.id
        super attributes
      end
    end

    def values
      v = []
      self.each{|id| v << ImEntity.find(id).value}
      v
    end

    def add! value
      item = ImEntity.create('value' => value)

      l = ImList.find(self.tail)
      if l.list.size < 100
        l.list << item.id
        l.save
      else
        nl = ImList.create('list' => [item.id])
        l.next = nl.id
        l.save
        self.tail = nl.id
        self.save
      end
    end

    def destroy!
      self.each{|id|ImEntity.find(id).destroy}

      l = ImList.find(self.head)
      loop do
        l.destroy
        if l.next
          l = ImList.find(l.next)
        else
          break
        end
      end
      self.destroy
    end

    def each
      l = ImList.find(self.head)
      loop do
        l.list.each{|item| yield item}
        if l.next
          l = ImList.find(l.next)
        else
          break
        end
      end
    end
  end

  class ImList < SimpleResource::Base
    include SimpleResource::TtEntityBackend
  end

  class ImEntity < SimpleResource::Base
    include SimpleResource::TtEntityBackend
  end
end
