# frozen-string-literal: true
#
# The pg_extended_date_support extension allows support
# for BC dates/timestamps by default, and infinite
# dates/timestamps if configured.  Without this extension,
# BC and infinite dates/timestamps will be handled incorrectly
# or raise an error.  This behavior isn't the default because
# it can hurt performance, and few users need support for BC
# and infinite dates/timestamps.
#
# To load the extension into the database:
#
#   DB.extension :pg_extended_date_support
#
# To enable support for infinite dates/timestamps:
#
#   DB.convert_infinite_timestamps = 'string' # or 'nil' or 'float'
#
# Related module: Sequel::Postgres::ExtendedDateSupport

#
module Sequel
  module Postgres
    module ExtendedDateSupport
      DATETIME_YEAR_1 = DateTime.new(1)
      TIME_YEAR_1 = Time.at(-62135596800).utc
      INFINITE_TIMESTAMP_STRINGS = ['infinity'.freeze, '-infinity'.freeze].freeze
      INFINITE_DATETIME_VALUES = ([PLUS_INFINITY, MINUS_INFINITY] + INFINITE_TIMESTAMP_STRINGS).freeze
      PLUS_DATE_INFINITY = Date::Infinity.new
      MINUS_DATE_INFINITY = -PLUS_DATE_INFINITY
      RATIONAL_60 = Rational(60)
      TIME_CAN_PARSE_BC = RUBY_VERSION >= '2.5'

      # Add dataset methods and update the conversion proces for dates and timestamps.
      def self.extended(db)
        db.extend_datasets(DatasetMethods)
        procs = db.conversion_procs
        procs[1082] = ::Sequel.method(:string_to_date)
        procs[1184] = procs[1114] = db.method(:to_application_timestamp)
        if ocps = db.instance_variable_get(:@oid_convertor_map)
          # Clear the oid convertor map entries for timestamps if they
          # exist, so it will regenerate new ones that use this extension.
          # This is only taken when using the jdbc adapter.
          Sequel.synchronize do
            ocps.delete(1184)
            ocps.delete(1114)
          end
        end
      end

      # Handle BC dates and times in bound variables. This is necessary for Date values
      # when using both the postgres and jdbc adapters, but also necessary for Time values
      # on jdbc.
      def bound_variable_arg(arg, conn)
        case arg
        when Time, Date
          @default_dataset.literal_date_or_time(arg)
        else
          super
        end
      end

      # Whether infinite timestamps/dates should be converted on retrieval.  By default, no
      # conversion is done, so an error is raised if you attempt to retrieve an infinite
      # timestamp/date.  You can set this to :nil to convert to nil, :string to leave
      # as a string, or :float to convert to an infinite float.
      attr_reader :convert_infinite_timestamps

      # Set whether to allow infinite timestamps/dates.  Make sure the
      # conversion proc for date reflects that setting.
      def convert_infinite_timestamps=(v)
        @convert_infinite_timestamps = case v
        when Symbol
          v
        when 'nil'
          :nil
        when 'string'
          :string
        when 'date'
          :date
        when 'float'
          :float
        when String, true
          typecast_value_boolean(v)
        else
          false
        end

        pr = old_pr = Sequel.method(:string_to_date)
        if @convert_infinite_timestamps
          pr = lambda do |val|
            case val
            when *INFINITE_TIMESTAMP_STRINGS
              infinite_timestamp_value(val)
            else
              old_pr.call(val)
            end
          end
        end
        add_conversion_proc(1082, pr)
      end

      # Handle BC dates in timestamps by moving the BC from after the time to
      # after the date, to appease ruby's date parser.
      # If convert_infinite_timestamps is true and the value is infinite, return an appropriate
      # value based on the convert_infinite_timestamps setting.
      def to_application_timestamp(value)
        if value.is_a?(String) && (m = /((?:[-+]\d\d:\d\d)(:\d\d)?)?( BC)?\z/.match(value)) && (m[2] || m[3])
          if m[3]
            value = value.sub(' BC', '').sub(' ', ' BC ')
          end
          if m[2]
            dt = if Sequel.datetime_class == DateTime
              DateTime.parse(value)
            elsif TIME_CAN_PARSE_BC
              Time.parse(value)
            # :nocov:
            else
              DateTime.parse(value).to_time
            # :nocov:
            end

            Sequel.convert_output_timestamp(dt, Sequel.application_timezone)
          else
            super(value)
          end
        elsif convert_infinite_timestamps
          case value
          when *INFINITE_TIMESTAMP_STRINGS
            infinite_timestamp_value(value)
          else
            super
          end
        else
          super
        end
      end

      private
      
      # Return an appropriate value for the given infinite timestamp string.
      def infinite_timestamp_value(value)
        case convert_infinite_timestamps
        when :nil
          nil
        when :string
          value
        when :date
          value == 'infinity' ? PLUS_DATE_INFINITY : MINUS_DATE_INFINITY
        else
          value == 'infinity' ? PLUS_INFINITY : MINUS_INFINITY
        end
      end
      
      # If the value is an infinite value (either an infinite float or a string returned by
      # by PostgreSQL for an infinite date), return it without converting it if
      # convert_infinite_timestamps is set.
      def typecast_value_date(value)
        if convert_infinite_timestamps
          case value
          when *INFINITE_DATETIME_VALUES
            value
          else
            super
          end
        else
          super
        end
      end

      # If the value is an infinite value (either an infinite float or a string returned by
      # by PostgreSQL for an infinite timestamp), return it without converting it if
      # convert_infinite_timestamps is set.
      def typecast_value_datetime(value)
        if convert_infinite_timestamps
          case value
          when *INFINITE_DATETIME_VALUES
            value
          else
            super
          end
        else
          super
        end
      end
        
      module DatasetMethods
        private

        # Handle BC Date objects.
        def literal_date(date)
          if date.year < 1
            date <<= ((date.year) * 24 - 12)
            date.strftime("'%Y-%m-%d BC'")
          else
            super
          end
        end

        # Handle BC DateTime objects.
        def literal_datetime(date)
          if date < DATETIME_YEAR_1
            date <<= ((date.year) * 24 - 12)
            date = db.from_application_timestamp(date)
            minutes = (date.offset * 1440).to_i
            date.strftime("'%Y-%m-%d %H:%M:%S.%6N#{sprintf("%+03i%02i", *minutes.divmod(60))} BC'")
          else
            super
          end
        end

        # Handle Date::Infinity values
        def literal_other_append(sql, v)
          if v.is_a?(Date::Infinity)
            sql << (v > 0 ? "'infinity'" : "'-infinity'")
          else
            super
          end
        end

        if RUBY_ENGINE == 'jruby'
          # :nocov:

          ExtendedDateSupport::CONVERT_TYPES = [Java::JavaSQL::Types::DATE, Java::JavaSQL::Types::TIMESTAMP]

          # Use non-JDBC parsing as JDBC parsing doesn't work for BC dates/timestamps.
          def type_convertor(map, meta, type, i)
            case type
            when *CONVERT_TYPES
              db.oid_convertor_proc(meta.getField(i).getOID)
            else
              super
            end
          end

          # Work around JRuby bug #4822 in Time#to_datetime for times before date of calendar reform
          def literal_time(time)
            if time < TIME_YEAR_1
              literal_datetime(DateTime.parse(super))
            else
              super
            end
          end
          # :nocov:
        else
          # Handle BC Time objects.
          def literal_time(time)
            if time < TIME_YEAR_1
              time = db.from_application_timestamp(time)
              time.strftime("'#{sprintf('%04i', time.year.abs+1)}-%m-%d %H:%M:%S.%6N#{sprintf("%+03i%02i", *(time.utc_offset/RATIONAL_60).divmod(60))} BC'")
            else
              super
            end
          end
        end
      end
    end
  end

  Database.register_extension(:pg_extended_date_support, Postgres::ExtendedDateSupport)
end
