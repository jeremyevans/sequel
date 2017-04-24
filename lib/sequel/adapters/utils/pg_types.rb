# frozen-string-literal: true

module Sequel
  module Postgres
    NAN             = 0.0/0.0
    PLUS_INFINITY   = 1.0/0.0
    MINUS_INFINITY  = -1.0/0.0

    NAN_STR             = 'NaN'.freeze
    Sequel::Deprecation.deprecate_constant(self, :NAN_STR)
    PLUS_INFINITY_STR   = 'Infinity'.freeze
    Sequel::Deprecation.deprecate_constant(self, :PLUS_INFINITY_STR)
    MINUS_INFINITY_STR  = '-Infinity'.freeze
    Sequel::Deprecation.deprecate_constant(self, :MINUS_INFINITY_STR)
    TRUE_STR = 't'.freeze
    Sequel::Deprecation.deprecate_constant(self, :TRUE_STR)
    DASH_STR = '-'.freeze
    Sequel::Deprecation.deprecate_constant(self, :DASH_STR)
    
    TYPE_TRANSLATOR = tt = Class.new do
      def boolean(s) s == 't' end
      def integer(s) s.to_i end
      def float(s) 
        case s
        when 'NaN'
          NAN
        when 'Infinity'
          PLUS_INFINITY
        when '-Infinity'
          MINUS_INFINITY
        else
          s.to_f 
        end
      end
      def date(s) ::Date.new(*s.split('-').map(&:to_i)) end
      def bytea(str)
        str = if str =~ /\A\\x/
          # PostgreSQL 9.0+ bytea hex format
          str[2..-1].gsub(/(..)/){|s| s.to_i(16).chr}
        else
          # Historical PostgreSQL bytea escape format
          str.gsub(/\\(\\|'|[0-3][0-7][0-7])/) {|s|
            if s.size == 2 then s[1,1] else s[1,3].oct.chr end
          }
        end
        ::Sequel::SQL::Blob.new(str)
      end
    end.new

    # Type OIDs for string types used by PostgreSQL.  These types don't
    # have conversion procs associated with them (since the data is
    # already in the form of a string).
    STRING_TYPES = [18, 19, 25, 1042, 1043]#.freeze # SEQUEL5

    # Hash with type name strings/symbols and callable values for converting PostgreSQL types.
    # Non-builtin types that don't have fixed numbers should use this to register
    # conversion procs.
    PG_NAMED_TYPES = {} unless defined?(PG_NAMED_TYPES)
    PG_NAMED__TYPES = PG_NAMED_TYPES
    Sequel::Deprecation.deprecate_constant(self, :PG_NAMED_TYPES)

    # Hash with integer keys and callable values for converting PostgreSQL types.
    PG_TYPES = {} unless defined?(PG_TYPES)

    {
      [16] => tt.method(:boolean),
      [17] => tt.method(:bytea),
      [20, 21, 23, 26] => tt.method(:integer),
      [700, 701] => tt.method(:float),
      [1700] => ::BigDecimal.method(:new),
      [1083, 1266] => ::Sequel.method(:string_to_time),
      [1082] => ::Sequel.method(:string_to_date),
      [1184, 1114] => ::Sequel.method(:database_to_application_timestamp),
    }.each do |k,v|
      k.each{|n| PG_TYPES[n] = v}
    end
  end
end 
