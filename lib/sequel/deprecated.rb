module Sequel
  # This module makes it easy to print deprecation warnings with optional backtraces to a given stream.
  # There are a couple of methods you can use to change where the deprecation methods are printed
  # and whether they should include backtraces:
  #
  #   Sequel.Deprecation.output = $stderr # print deprecation messages to standard error (default)
  #   Sequel.Deprecation.output = File.open('deprecated_calls.txt', 'wb') # use a file instead
  #   Sequel.Deprecation.backtraces = false # don't include backtraces
  #   Sequel.Deprecation.backtraces = true # include full backtraces
  #   Sequel.Deprecation.backtraces = 10 # include 10 backtrace lines (default)
  #   Sequel.Deprecation.backtraces = 1 # include 1 backtrace line
  module Deprecation
    extend Metaprogramming

    @output = $stderr
    @backtraces = 10

    metaattr_accessor :output, :backtraces

    # Print the message to the output stream
    def self.deprecate(method, instead=nil)
      message = instead ? "#{method} is deprecated and will be removed in a future version.  #{instead}." : method
      return unless output
      output.puts(message)
      case backtraces
      when Integer
        b = backtraces
        caller.each do |c|
          b -= 1
          output.puts(c)
          break if b == 0
        end
      when true
        caller.each{|c| output.puts(c)}
      end
    end
  end

  def self.open(*args, &block)
    Deprecation.deprecate('Sequel.open', 'Use Sequel.connect')
    Database.connect(*args, &block)
  end

  def self.upcase_identifiers=(value)
    Deprecation.deprecate('Sequel.upcase_identifiers=', 'Use Sequel.identifier_input_method = :upcase or nil')
    Database.identifier_input_method = value ? :upcase : nil
  end

  def self.use_parse_tree
    Deprecation.deprecate('Sequel.use_parse_tree', 'Sequel has not used ParseTree since 2.2')
    false
  end

  def self.use_parse_tree=(val)
    Deprecation.deprecate('Sequel.use_parse_tree=', 'Sequel has not used ParseTree since 2.2')
    raise(Error, 'ParseTree support has been removed from Sequel') if val
  end

  class Database
    def self.upcase_identifiers=(value)
      Deprecation.deprecate('Sequel::Database.upcase_identifiers=', 'Use Sequel::Database.identifier_input_method = :upcase or nil')
      self.identifier_input_method = value ? :upcase : nil
    end

    def upcase_identifiers=(v)
      Deprecation.deprecate('Sequel::Database#upcase_identifiers=', 'Use Sequel::Database#identifier_input_method = :upcase or nil')
      self.identifier_input_method = v ? :upcase : nil
    end

    def upcase_identifiers?
      Deprecation.deprecate('Sequel::Database#upcase_identifiers?', 'Use Sequel::Database#identifier_input_method == :upcase')
      identifier_input_method == :upcase
    end

    def >>(*args, &block)
      Deprecation.deprecate('Sequel::Database#>>', 'Use Sequel::Database#fetch')
      fetch(*args, &block)
    end

    def query(&block)
      Deprecation.deprecate('Sequel::Database#query', 'require "sequel/extensions/query" first')
      dataset.query(&block)
    end
  end

  class Dataset
    DATASET_CLASSES = []

    def self.dataset_classes
      Deprecation.deprecate('Sequel::Dataset#dataset_classes', 'No replacement is planned')
      DATASET_CLASSES
    end

    def self.inherited(c)
      DATASET_CLASSES << c
    end

    def upcase_identifiers=(v)
      Deprecation.deprecate('Sequel::Dataset#upcase_identifiers=', 'Use Sequel::Dataset#identifier_input_method = :upcase or nil')
      @identifier_input_method = v ? :upcase : nil
    end

    def upcase_identifiers?
      Deprecation.deprecate('Sequel::Dataset#upcase_identifiers?', 'Use Sequel::Dataset#identifier_input_method == :upcase')
      @identifier_input_method == :upcase
    end

    def model_classes
      Deprecation.deprecate('Sequel::Dataset#model_classes', 'Sequel::Model datasets no longer set this information')
      @opts[:models]
    end

    def polymorphic_key
      Deprecation.deprecate('Sequel::Dataset#polymorphic_key', 'Sequel::Model datasets no longer set this information')
      @opts[:polymorphic_key]
    end

    def set_model(key, *args)
      Deprecation.deprecate('Sequel::Dataset#set_model', 'Use Sequel::Dataset#set_row_proc with an appropriate row proc')
      # This code is more verbose then necessary for performance reasons
      case key
      when nil # set_model(nil) => no argument provided, so the dataset is denuded
        @opts.merge!(:naked => true, :models => nil, :polymorphic_key => nil)
        self.row_proc = nil
      when Class
        # isomorphic model
        @opts.merge!(:naked => nil, :models => {nil => key}, :polymorphic_key => nil)
        if key.respond_to?(:load)
          # the class has a values setter method, so we use it
          self.row_proc = proc{|h| key.load(h, *args)}
        else
          # otherwise we just pass the hash to the constructor
          self.row_proc = proc{|h| key.new(h, *args)}
        end
      when Symbol
        # polymorphic model
        hash = args.shift || raise(ArgumentError, "No class hash supplied for polymorphic model")
        @opts.merge!(:naked => true, :models => hash, :polymorphic_key => key)
        if (hash.empty? ? (hash[nil] rescue nil) : hash.values.first).respond_to?(:load)
          # the class has a values setter method, so we use it
          self.row_proc = proc do |h|
            c = hash[h[key]] || hash[nil] || \
              raise(Error, "No matching model class for record (#{polymorphic_key} => #{h[polymorphic_key].inspect})")
            c.load(h, *args)
          end
        else
          # otherwise we just pass the hash to the constructor
          self.row_proc = proc do |h|
            c = hash[h[key]] || hash[nil] || \
              raise(Error, "No matching model class for record (#{polymorphic_key} => #{h[polymorphic_key].inspect})")
            c.new(h, *args)
          end
        end
      else
        raise ArgumentError, "Invalid model specified"
      end
      self
    end

    def create_view(name)
      Sequel::Deprecation.deprecate('Sequel::Dataset#create_view', 'Use Sequel::Database#create_view')
      @db.create_view(name, self)
    end

    def create_or_replace_view(name)
      Sequel::Deprecation.deprecate('Sequel::Dataset#create_or_replace_view', 'Use Sequel::Database#create_or_replace_view')
      @db.create_or_replace_view(name, self)
    end

    def import(*args, &block)
      Sequel::Deprecation.deprecate('Sequel::Dataset#import', 'Use Sequel::Dataset#multi_insert')
      multi_insert(*args, &block)
    end

    def size
      Sequel::Deprecation.deprecate('Sequel::Dataset#size', 'Use Sequel::Dataset#count')
      count
    end

    def uniq(*args)
      Sequel::Deprecation.deprecate('Sequel::Dataset#uniq', 'Use Sequel::Dataset#distinct')
      distinct(*args)
    end

    def quote_column_ref(name)
      Sequel::Deprecation.deprecate('Sequel::Dataset#quote_column_ref', 'Use Sequel::Dataset#quote_identifier')
      quote_identifier(name)
    end

    def symbol_to_column_ref(sym)
      Sequel::Deprecation.deprecate('Sequel::Dataset#symbol_to_column_ref', 'Use Sequel::Dataset#literal')
      literal_symbol(sym)
    end

    def paginate(page_no, page_size, record_count=nil)
      Sequel::Deprecation.deprecate('Sequel::Dataset#paginate', 'require "sequel/extensions/pagination" first')
      require "sequel/extensions/pagination"
      raise(Error, "You cannot paginate a dataset that already has a limit") if @opts[:limit]
      paginated = limit(page_size, (page_no - 1) * page_size)
      paginated.extend(Pagination)
      paginated.set_pagination_info(page_no, page_size, record_count || count)
    end

    def each_page(page_size, &block)
      Sequel::Deprecation.deprecate('Sequel::Dataset#each_page', 'require "sequel/extensions/pagination" first')
      raise(Error, "You cannot paginate a dataset that already has a limit") if @opts[:limit]
      record_count = count
      total_pages = (record_count / page_size.to_f).ceil
      (1..total_pages).each{|page_no| yield paginate(page_no, page_size, record_count)}
      self
    end

    def query(&block)
      Sequel::Deprecation.deprecate('Sequel::Dataset#each_page', 'require "sequel/extensions/query" first')
      require "sequel/extensions/query"
      copy = clone({})
      copy.extend(QueryBlockCopy)
      copy.instance_eval(&block)
      clone(copy.opts)
    end

    def print(*cols)
      Sequel::Deprecation.deprecate('Sequel::Dataset#print', 'require "sequel/extensions/pretty_table" first')
      Sequel::PrettyTable.print(naked.all, cols.empty? ? columns : cols)
    end
  end

  module SQL
    module CastMethods
      def cast_as(sql_type)
        Sequel::Deprecation.deprecate('Sequel::SQL::CastMethods#cast_as', 'Use Sequel::SQL::CastMethods#cast')
        cast(sql_type)
      end
    end

    class Blob
      def to_blob(*args)
        Sequel::Deprecation.deprecate('Sequel::SQL::Blob#to_blob', 'Use Sequel::SQL::Blob#to_sequel_blob')
        to_sequel_blob(*args)
      end
    end
  end

  module PrettyTable
    def self.print(*args)
      Sequel::Deprecation.deprecate('Sequel::PrettyTable#print', 'require "sequel/extensions/pretty_table" first')
      require "sequel/extensions/pretty_table"
      print(*args)
    end
  end
end

if RUBY_VERSION < '1.9.0'
  class Hash
    unless method_defined?(:key)
      def key(*args, &block)
        Sequel::Deprecation.deprecate('Hash#key', 'Use Hash#index')
        index(*args, &block)
      end
    end
  end
end

module Enumerable
  # Invokes the specified method for each item, along with the supplied
  # arguments.
  def send_each(sym, *args)
    Sequel::Deprecation.deprecate('Enumerable#send_each', 'Use Enumerable#each{|x| x.send(...)}')
    each{|i| i.send(sym, *args)}
  end
end

class Range
  def interval
    Sequel::Deprecation.deprecate('Range#interval', 'Use range.first - range.last - (range.exclude_end? ? 1 : 0)')
    last - first - (exclude_end? ? 1 : 0)
  end
end

class Array
  def extract_options!
    Sequel::Deprecation.deprecate('Array#extract_options!', 'Use array.last.is_a?(Hash) ? array.pop : {}')
    last.is_a?(Hash) ? pop : {}
  end 

  def to_sql
    Sequel::Deprecation.deprecate('Array#to_sql', 'It may be a good idea to refactor your implementation so this type of method is not required')
    map {|l| ((m = /^(.*)--/.match(l)) ? m[1] : l).chomp}.join(' '). \
      gsub(/\/\*.*\*\//, '').gsub(/\s+/, ' ').strip
  end
end

class String
  def split_sql
    Sequel::Deprecation.deprecate('String#split_sql', 'It may be a good idea to refactor your implementation so this type of method is not required')
    to_sql.split(';').map {|s| s.strip}
  end

  def to_sql
    Sequel::Deprecation.deprecate('String#to_sql', 'It may be a good idea to refactor your implementation so this type of method is not required')
    split("\n").to_sql
  end

  def expr(*args)
    Sequel::Deprecation.deprecate('String#expr', 'Use String#lit')
    lit(*args)
  end

  def to_blob(*args)
    Sequel::Deprecation.deprecate('String#to_blob', 'Use String#to_sequel_blob')
    to_sequel_blob(*args)
  end

  def to_date
    Sequel::Deprecation.deprecate('String#to_date', 'You should require "sequel/extensions/string_date_time"')
    begin
      Date.parse(self, Sequel.convert_two_digit_years)
    rescue => e
      raise Sequel::Error::InvalidValue, "Invalid Date value '#{self}' (#{e.message})"
    end 
  end 

  def to_datetime
    Sequel::Deprecation.deprecate('String#to_datetime', 'You should require "sequel/extensions/string_date_time"')
    begin
      DateTime.parse(self, Sequel.convert_two_digit_years)
    rescue => e
      raise Sequel::Error::InvalidValue, "Invalid DateTime value '#{self}' (#{e.message})"
    end 
  end 

  def to_sequel_time
    Sequel::Deprecation.deprecate('String#to_sequel_time', 'You should require "sequel/extensions/string_date_time"')
    begin
      if Sequel.datetime_class == DateTime
        DateTime.parse(self, Sequel.convert_two_digit_years)
      else
        Sequel.datetime_class.parse(self)
      end 
    rescue => e
      raise Sequel::Error::InvalidValue, "Invalid #{Sequel.datetime_class} value '#{self}' (#{e.message})"
    end 
  end 

  def to_time
    Sequel::Deprecation.deprecate('String#to_time', 'You should require "sequel/extensions/string_date_time"')
    begin
      Time.parse(self)
    rescue => e
      raise Sequel::Error::InvalidValue, "Invalid Time value '#{self}' (#{e.message})"
    end 
  end
end

class Symbol
  def |(sub)
    return super unless (Integer === sub) || ((Array === sub) && sub.any?{|x| Integer === x})
    Sequel::Deprecation.deprecate('The use of Symbol#| for SQL array subscripts', 'Use Symbol#sql_subscript')
    Sequel::SQL::Subscript.new(self, Array(sub))
  end

  def to_column_ref(ds)
    Sequel::Deprecation.deprecate('Symbol#to_column_ref', 'Use Dataset#literal')
    ds.literal(self)
  end
end

class Module
  unless method_defined?(:class_def)
   def class_def(name, &block)
      Sequel::Deprecation.deprecate('Object#class_def', "Install the metaid gem")
      class_eval{define_method(name, &block)}
    end
  end

  private

  def class_attr_overridable(*meths)
    Sequel::Deprecation.deprecate('Module#class_attr_overridable', "Copy the method from #{__FILE__} (line #{__LINE__}) if you need it")
    meths.each{|meth| class_eval("def #{meth}; !defined?(@#{meth}) ? (@#{meth} = self.class.#{meth}) : @#{meth} end")}
    attr_writer(*meths)
  end

  def class_attr_reader(*meths)
    Sequel::Deprecation.deprecate('Module#class_attr_reader', "Copy the method from #{__FILE__} (line #{__LINE__}) if you need it")
    meths.each{|meth| define_method(meth){self.class.send(meth)}}
  end

  def metaalias(to, from)
    Sequel::Deprecation.deprecate('Module#metaalias', "Copy the method from #{__FILE__} (line #{__LINE__}) if you need it")
    meta_eval{alias_method to, from}
  end

  def metaattr_accessor(*meths)
    Sequel::Deprecation.deprecate('Module#metaattr_accessor', "Copy the method from #{__FILE__} (line #{__LINE__}) if you need it")
    meta_eval{attr_accessor(*meths)}
  end

  def metaattr_reader(*meths)
    Sequel::Deprecation.deprecate('Module#metaattr_reader', "Copy the method from #{__FILE__} (line #{__LINE__}) if you need it")
    meta_eval{attr_reader(*meths)}
  end
end

class Object
  def is_one_of?(*classes)
    Sequel::Deprecation.deprecate('Object#is_one_of?', "Use classes.any?{|c| object.is_a?(c)}")
    classes.any?{|c| is_a?(c)}
  end

  unless method_defined?(:meta_def)
    def meta_def(name, &block)
      Sequel::Deprecation.deprecate('Object#meta_def', "Install the metaid gem")
      meta_eval{define_method(name, &block)}
    end
  end
  
  unless method_defined?(:meta_eval)
    def meta_eval(&block)
      Sequel::Deprecation.deprecate('Object#meta_eval', "Install the metaid gem")
      metaclass.instance_eval(&block)
    end
  end
  
  unless method_defined?(:metaclass)
    def metaclass
      Sequel::Deprecation.deprecate('Object#metaclass', "Install the metaid gem")
      class << self
        self
      end
    end
  end
end

class FalseClass
  unless method_defined?(:blank?)
    def blank?
      Sequel::Deprecation.deprecate('FalseClass#blank?', "require 'sequel/extensions/blank' first")
      true
    end
  end
end

class NilClass
  unless method_defined?(:blank?)
    def blank?
      Sequel::Deprecation.deprecate('NilClass#blank?', "require 'sequel/extensions/blank' first")
      true
    end
  end
end

class Numeric
  unless method_defined?(:blank?)
    def blank?
      Sequel::Deprecation.deprecate('Numeric#blank?', "require 'sequel/extensions/blank' first")
      false
    end
  end
end

class String
  unless method_defined?(:blank?)
    def blank?
      Sequel::Deprecation.deprecate('String#blank?', "require 'sequel/extensions/blank' first")
      strip.empty?
    end
  end
end

class TrueClass
  unless method_defined?(:blank?)
    def blank?
      Sequel::Deprecation.deprecate('FalseClass#blank?', "require 'sequel/extensions/blank' first")
      false
    end
  end
end

# Helpers from Metaid and a bit more
class Object
  unless method_defined?(:blank?)
    def blank?
      Sequel::Deprecation.deprecate('FalseClass#blank?', "require 'sequel/extensions/blank' first")
      respond_to?(:empty?) && empty?
    end
  end
end

