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
  end

  class Dataset
    def upcase_identifiers=(v)
      Deprecation.deprecate('Sequel::Dataset#upcase_identifiers=', 'Use Sequel::Dataset#identifier_input_method = :upcase or nil')
      @identifier_input_method = v ? :upcase : nil
    end

    def upcase_identifiers?
      Deprecation.deprecate('Sequel::Dataset#upcase_identifiers?', 'Use Sequel::Dataset#identifier_input_method == :upcase')
      @identifier_input_method == :upcase
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

