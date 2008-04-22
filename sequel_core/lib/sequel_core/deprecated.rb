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

    def deprecate(meth, message = nil)
      ::Sequel::Deprecation.deprecate("#{meth} is deprecated, and will be removed in Sequel 2.0.#{"  #{message}." if message}")
    end
  end
  
  class << self
    include Sequel::Deprecation
    def method_missing(m, *args)
      deprecate("Sequel.method_missing", "You should define Sequel.#{m} for the adapter.")
      c = Database.adapter_class(m)
      begin
        # three ways to invoke this:
        # 0 arguments: Sequel.dbi
        # 1 argument:  Sequel.dbi(db_name)
        # more args:   Sequel.dbi(db_name, opts)
        case args.size
        when 0
          opts = {}
        when 1
          opts = args[0].is_a?(Hash) ? args[0] : {:database => args[0]}
        else
          opts = args[1].merge(:database => args[0])
        end
      rescue
        raise Error::AdapterNotFound, "Unknown adapter (#{m})"
      end
      c.new(opts)
    end
  end

  class Dataset
    include Deprecation

    MUTATION_RE = /^(.+)!$/.freeze

    def clone_merge(opts = {})
      deprecate("Sequel::Dataset#clone", "Use clone")
      clone(opts)
    end

    def set_options(opts)
      deprecate("Sequel::Dataset#set_options")
      @opts = opts
      @columns = nil
    end

    def set_row_proc(&filter)
      deprecate("Sequel::Dataset#set_row_proc", "Use row_proc=")
      @row_proc = filter
    end

    def remove_row_proc
      deprecate("Sequel::Dataset#remove_row_proc", "Use row_proc=nil")
      @row_proc = nil
    end

    # Provides support for mutation methods (filter!, order!, etc.) and magic
    # methods.
    def method_missing(m, *args, &block)
      if m.to_s =~ MUTATION_RE
        meth = $1.to_sym
        super unless respond_to?(meth)
        copy = send(meth, *args, &block)
        super if copy.class != self.class
        deprecate("Sequel::Dataset#method_missing", "Define Sequel::Dataset##{m}, or use Sequel::Dataset.def_mutation_method(:#{meth})")
        @opts.merge!(copy.opts)
        self
      elsif magic_method_missing(m)
        send(m, *args)
      else
         super
      end
    end

    MAGIC_METHODS = {
      /^order_by_(.+)$/   => proc {|c| proc {deprecate("Sequel::Dataset#method_missing", "Use order(#{c.inspect}) or define order_by_#{c}"); order(c)}},
      /^first_by_(.+)$/   => proc {|c| proc {deprecate("Sequel::Dataset#method_missing", "Use order(#{c.inspect}).first or define first_by_#{c}"); order(c).first}},
      /^last_by_(.+)$/    => proc {|c| proc {deprecate("Sequel::Dataset#method_missing", "Use order(#{c.inspect}).last or define last_by_#{c}"); order(c).last}},
      /^filter_by_(.+)$/  => proc {|c| proc {|v| deprecate("Sequel::Dataset#method_missing", "Use filter(#{c.inspect}=>#{v.inspect}) or define filter_by_#{c}"); filter(c => v)}},
      /^all_by_(.+)$/     => proc {|c| proc {|v| deprecate("Sequel::Dataset#method_missing", "Use filter(#{c.inspect}=>#{v.inspect}).all or define all_by_#{c}"); filter(c => v).all}},
      /^find_by_(.+)$/    => proc {|c| proc {|v| deprecate("Sequel::Dataset#method_missing", "Use filter(#{c.inspect}=>#{v.inspect}).find or define find_by_#{c}"); filter(c => v).first}},
      /^group_by_(.+)$/   => proc {|c| proc {deprecate("Sequel::Dataset#method_missing", "Use group(#{c.inspect}) or define group_by_#{c}"); group(c)}},
      /^count_by_(.+)$/   => proc {|c| proc {deprecate("Sequel::Dataset#method_missing", "Use group_and_count(#{c.inspect}) or define count_by_#{c})"); group_and_count(c)}}
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
  
  module SQL
    module DeprecatedColumnMethods
      AS = 'AS'.freeze
      DESC = 'DESC'.freeze
      ASC = 'ASC'.freeze

      def as(a)
        Sequel::Deprecation.deprecate("Object#as is deprecated and will be removed in Sequel 2.0.  Use Symbol#as or String#as.")
        ColumnExpr.new(self, AS, a)
      end
      def AS(a)
        Sequel::Deprecation.deprecate("Object#AS is deprecated and will be removed in Sequel 2.0.  Use Symbol#as or String#as.")
        ColumnExpr.new(self, AS, a)
      end
      def desc
        Sequel::Deprecation.deprecate("Object#desc is deprecated and will be removed in Sequel 2.0.  Use Symbol#desc or String#desc.")
        ColumnExpr.new(self, DESC)
      end
      def DESC
        Sequel::Deprecation.deprecate("Object#DESC is deprecated and will be removed in Sequel 2.0.  Use Symbol#desc or String#desc.")
        ColumnExpr.new(self, DESC)
      end
      def asc
        Sequel::Deprecation.deprecate("Object#asc is deprecated and will be removed in Sequel 2.0.  Use Symbol#asc or String#asc.")
        ColumnExpr.new(self, ASC)
      end
      def ASC
        Sequel::Deprecation.deprecate("Object#ASC is deprecated and will be removed in Sequel 2.0.  Use Symbol#asc or String#asc.")
        ColumnExpr.new(self, ASC)
      end
      def all
        Sequel::Deprecation.deprecate("Object#all is deprecated and will be removed in Sequel 2.0.  Use :#{self}.* or '#{self}.*'.lit.")
        Sequel::SQL::ColumnAll.new(self)
      end
      def ALL
        Sequel::Deprecation.deprecate("Object#ALL is deprecated and will be removed in Sequel 2.0.  Use :#{self}.* or '#{self}.*'.lit.")
        Sequel::SQL::ColumnAll.new(self)
      end

      def cast_as(t)
        Sequel::Deprecation.deprecate("Object#cast_as is deprecated and will be removed in Sequel 2.0.  Use Symbol#cast_as or String#cast_as.")
        if t.is_a?(Symbol)
          t = t.to_s.lit
        end
        Sequel::SQL::Function.new(:cast, self.as(t))
      end
    end
  end
end

class Object
  include Sequel::SQL::DeprecatedColumnMethods
  def Sequel(*args)
    Sequel::Deprecation.deprecate("Object#Sequel is deprecated and will be removed in Sequel 2.0.  Use Sequel.connect.")
    Sequel.connect(*args)
  end
  def rollback!
    Sequel::Deprecation.deprecate("Object#rollback! is deprecated and will be removed in Sequel 2.0.  Use raise Sequel::Error::Rollback.")
    raise Sequel::Error::Rollback
  end
end

class Symbol
  # Converts missing method calls into functions on columns, if the
  # method name is made of all upper case letters.
  def method_missing(sym, *args)
    if ((s = sym.to_s) =~ /^([A-Z]+)$/)
      Sequel::Deprecation.deprecate("Symbol#method_missing is deprecated and will be removed in Sequel 2.0.  Use :#{sym}[:#{self}].")
      Sequel::SQL::Function.new(s.downcase, self)
    else
      super
    end
  end
end
