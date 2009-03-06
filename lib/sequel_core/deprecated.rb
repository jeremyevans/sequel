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
