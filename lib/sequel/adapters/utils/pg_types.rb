module Sequel
  module Postgres
    NAN             = 0.0/0.0
    PLUS_INFINITY   = 1.0/0.0
    MINUS_INFINITY  = -1.0/0.0
    NAN_STR             = 'NaN'.freeze
    PLUS_INFINITY_STR   = 'Infinity'.freeze
    MINUS_INFINITY_STR  = '-Infinity'.freeze
    TRUE_STR = 't'.freeze
    DASH_STR = '-'.freeze
    
    TYPE_TRANSLATOR = tt = Class.new do
      def boolean(s) s == TRUE_STR end
      def bytea(s) ::Sequel::SQL::Blob.new(Adapter.unescape_bytea(s)) end
      def integer(s) s.to_i end
      def float(s) 
        case s
        when NAN_STR
          NAN
        when PLUS_INFINITY_STR
          PLUS_INFINITY
        when MINUS_INFINITY_STR
          MINUS_INFINITY
        else
          s.to_f 
        end
      end
      def date(s) ::Date.new(*s.split(DASH_STR).map{|x| x.to_i}) end
    end.new

    # Hash with type name strings/symbols and callable values for converting PostgreSQL types.
    # Non-builtin types that don't have fixed numbers should use this to register
    # conversion procs.
    PG_NAMED_TYPES = {} unless defined?(PG_NAMED_TYPES)

    # Hash with integer keys and callable values for converting PostgreSQL types.
    PG_TYPES = {} unless defined?(PG_TYPES)

    {
      [16] => tt.method(:boolean),
      [17] => tt.method(:bytea),
      [20, 21, 23, 26] => tt.method(:integer),
      [700, 701] => tt.method(:float),
      [1700] => ::BigDecimal.method(:new),
      [1083, 1266] => ::Sequel.method(:string_to_time),
    }.each do |k,v|
      k.each{|n| PG_TYPES[n] = v}
    end
    
    class << self
      # As an optimization, Sequel sets the date style to ISO, so that PostgreSQL provides
      # the date in a known format that Sequel can parse faster.  This can be turned off
      # if you require a date style other than ISO.
      attr_reader :use_iso_date_format
    end

    # Modify the type translator for the date type depending on the value given.
    def self.use_iso_date_format=(v)
      PG_TYPES[1082] = v ? TYPE_TRANSLATOR.method(:date) : Sequel.method(:string_to_date)
      @use_iso_date_format = v
    end
    self.use_iso_date_format = true
  end
end 
