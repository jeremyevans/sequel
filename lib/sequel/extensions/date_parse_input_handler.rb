# frozen-string-literal: true
#
# The date_parse_input_handler extension allows for configuring how input
# to date parsing methods should be handled.  By default, the
# extension does not change behavior.  However, you can use the
# +Sequel.date_parse_input_handler+ method to support custom handling
# of input strings to the date parsing methods.  For example, if you want
# to implement a length check to prevent denial of service vulnerabilities
# in older versions of Ruby, you can do:
#
#   Sequel.extension :date_parse_input_handler
#   Sequel.date_parse_input_handler do |string|
#     raise Sequel::InvalidValue, "string length (200) exceeds the limit 128" if string.bytesize > 128
#     string
#   end
#
# You can also use +Sequel.date_parse_input_handler+ to modify the string
# that will be passed to the parsing methods.  For example, you could
# truncate it:
#
#   Sequel.date_parse_input_handler do |string|
#     string.b[0, 128]
#   end
#
# Be aware that modern versions of Ruby will raise an exception if
# date parsing input exceeds 128 bytes.

module Sequel
  module DateParseInputHandler
    def date_parse_input_handler(&block)
      singleton_class.class_eval do
        define_method(:handle_date_parse_input, &block)
        private :handle_date_parse_input
        alias handle_date_parse_input handle_date_parse_input
      end
    end

    # Call date parse input handler with input string.
    def string_to_date(string)
      super(handle_date_parse_input(string))
    end

    # Call date parse input handler with input string.
    def string_to_datetime(string)
      super(handle_date_parse_input(string))
    end

    # Call date parse input handler with input string.
    def string_to_time(string)
      super(handle_date_parse_input(string))
    end

    private

    # Call date parse input handler with input string.
    def _date_parse(string)
      super(handle_date_parse_input(string))
    end

    # Return string as-is by default, so by default behavior does not change.
    def handle_date_parse_input(string)
      string
    end
  end

  extend DateParseInputHandler
end
