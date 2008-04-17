module Sequel
  module Deprecation
    def self.deprecation_message_stream=(file)
      @dms = file
    end

    def self.print_tracebacks=(pt)
      @pt = pt
    end

    def self.deprecate(message)
      if @dms
        @dms.puts(message)
        caller.each{|c| @dms.puts(c)} if @pt 
      end
    end

    def deprecate(meth, message)
      ::Sequel::Deprecation.deprecate("#{meth} #{message}. #{meth} is deprecated, and will be removed in Sequel 2.0.")
    end
  end

  class Dataset
    include Deprecation

    MUTATION_RE = /^(.+)!$/.freeze

    # Provides support for mutation methods (filter!, order!, etc.) and magic
    # methods.
    def method_missing(m, *args, &block)
      if m.to_s =~ MUTATION_RE
        meth = $1.to_sym
        super unless respond_to?(meth)
        copy = send(meth, *args, &block)
        super if copy.class != self.class
        deprecate(m, "is not a defined method")
        @opts.merge!(copy.opts)
        self
      elsif magic_method_missing(m)
        send(m, *args)
      else
         super
      end
    end

    MAGIC_METHODS = {
      /^order_by_(.+)$/   => proc {|c| proc {deprecate("order_by_#{c}", "is not a defined_method, please use order(#{c.inspect})"); order(c)}},
      /^first_by_(.+)$/   => proc {|c| proc {deprecate("first_by_#{c}", "is not a defined_method, please use order(#{c.inspect}).first"); order(c).first}},
      /^last_by_(.+)$/    => proc {|c| proc {deprecate("last_by_#{c}", "is not a defined_method, please use order(#{c.inspect}).last"); order(c).last}},
      /^filter_by_(.+)$/  => proc {|c| proc {|v| deprecate("filter_by_#{c}", "is not a defined_method, please use filter(#{c.inspect}=>#{v.inspect})"); filter(c => v)}},
      /^all_by_(.+)$/     => proc {|c| proc {|v| deprecate("all_by_#{c}", "is not a defined_method, please use filter(#{c.inspect}=>#{v.inspect}).all"); filter(c => v).all}},
      /^find_by_(.+)$/    => proc {|c| proc {|v| deprecate("find_by_#{c}", "is not a defined_method, please use filter(#{c.inspect}=>#{v.inspect}).find"); filter(c => v).first}},
      /^group_by_(.+)$/   => proc {|c| proc {deprecate("group_by_#{c}", "is not a defined_method, please use group(#{c.inspect})"); group(c)}},
      /^count_by_(.+)$/   => proc {|c| proc {deprecate("count_by_#{c}", "is not a defined_method, please use group_and_count(#{c.inspect})"); group_and_count(c)}}
    }

    # Checks if the given method name represents a magic method and
    # defines it. Otherwise, nil is returned.
    def magic_method_missing(m)
      method_name = m.to_s
      MAGIC_METHODS.each_pair do |r, p|
        if method_name =~ r
          impl = p[$1.to_sym]
          return Dataset.class_def(m, &impl)
        end
      end
      nil
    end

  end
end
