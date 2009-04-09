module Sequel
  # This module makes it easy to print deprecation warnings with optional backtraces to a given stream.
  # There are a couple of methods you can use to change where the deprecation methods are printed
  # and whether they should include backtraces:
  #
  #   Sequel::Deprecation.output = $stderr # print deprecation messages to standard error (default)
  #   Sequel::Deprecation.output = File.open('deprecated_calls.txt', 'wb') # use a file instead
  #   Sequel::Deprecation.backtraces = false # don't include backtraces
  #   Sequel::Deprecation.backtraces = true # include full backtraces
  #   Sequel::Deprecation.backtraces = 10 # include 10 backtrace lines (default)
  #   Sequel::Deprecation.backtraces = 1 # include 1 backtrace line
  module Deprecation
    extend Metaprogramming

    @output = $stderr
    @backtraces = 10

    metaattr_accessor :output, :backtraces

    # Print the message to the output stream
    def self.deprecate(method, instead=nil)
      message = instead ? "#{method} is deprecated and will be removed in Sequel 3.0.  #{instead}." : method
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
end
