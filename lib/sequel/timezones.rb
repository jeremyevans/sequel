module Sequel
  @application_timezone = nil
  @database_timezone = nil
  @typecast_timezone = nil
  
  # Sequel doesn't pay much attention to timezones by default, but you can set it
  # handle timezones if you want.  There are three separate timezone settings, application_timezone,
  # database_timezone, and typecast_timezone.  All three timezones have getter and setter methods.
  # You can set all three timezones to the same value at once via <tt>Sequel.default_timezone=</tt>.
  #
  # The only timezone values that are supported by default are <tt>:utc</tt> (convert to UTC),
  # <tt>:local</tt> (convert to local time), and +nil+ (don't convert).  If you need to
  # convert to a specific timezone, or need the timezones being used to change based
  # on the environment (e.g. current user), you need to use the +named_timezones+ extension (and use
  # +DateTime+ as the +datetime_class+). Sequel also ships with a +thread_local_timezones+ extensions
  # which allows each thread to have its own timezone values for each of the timezones.
  module Timezones
    # The timezone you want the application to use.  This is the timezone
    # that incoming times from the database and typecasting are converted to.
    attr_reader :application_timezone
    
    # The timezone for storage in the database.  This is the
    # timezone to which Sequel will convert timestamps before literalizing them
    # for storage in the database.  It is also the timezone that Sequel will assume
    # database timestamp values are already in (if they don't include an offset).
    attr_reader :database_timezone
    
    # The timezone that incoming data that Sequel needs to typecast
    # is assumed to be already in (if they don't include an offset).
    attr_reader :typecast_timezone
  
    %w'application database typecast'.each do |t|
      class_eval("def #{t}_timezone=(tz); @#{t}_timezone = convert_timezone_setter_arg(tz) end", __FILE__, __LINE__)
    end
  
    # Convert the given +Time+/+DateTime+ object into the database timezone, used when
    # literalizing objects in an SQL string.
    def application_to_database_timestamp(v)
      convert_output_timestamp(v, Sequel.database_timezone)
    end

    # Converts the object to the given +output_timezone+.
    def convert_output_timestamp(v, output_timezone)
      if output_timezone
        if v.is_a?(DateTime)
          case output_timezone
          when :utc
            v.new_offset(0)
          when :local
            v.new_offset(local_offset_for_datetime(v))
          else
            convert_output_datetime_other(v, output_timezone)
          end
        else
          v.send(output_timezone == :utc ? :getutc : :getlocal)
        end
      else
        v
      end
    end
    
    # Converts the given object from the given input timezone to the
    # +application_timezone+ using +convert_input_timestamp+ and
    # +convert_output_timestamp+.
    def convert_timestamp(v, input_timezone)
      begin
        if v.is_a?(Date) && !v.is_a?(DateTime)
          # Dates handled specially as they are assumed to already be in the application_timezone
          if datetime_class == DateTime
            DateTime.civil(v.year, v.month, v.day, 0, 0, 0, application_timezone == :local ? (defined?(Rational) ? Rational(Time.local(v.year, v.month, v.day).utc_offset, 86400) : Time.local(v.year, v.month, v.day).utc_offset/86400.0) : 0)
          else
            Time.send(application_timezone == :utc ? :utc : :local, v.year, v.month, v.day)
          end
        else
          convert_output_timestamp(convert_input_timestamp(v, input_timezone), application_timezone)
        end
      rescue InvalidValue
        raise
      rescue => e
        raise convert_exception_class(e, InvalidValue)
      end
    end
    
    # Convert the given object into an object of <tt>Sequel.datetime_class</tt> in the
    # +application_timezone+.  Used when converting datetime/timestamp columns
    # returned by the database.
    def database_to_application_timestamp(v)
      convert_timestamp(v, Sequel.database_timezone)
    end
  
    # Sets the database, application, and typecasting timezones to the given timezone. 
    def default_timezone=(tz)
      self.database_timezone = tz
      self.application_timezone = tz
      self.typecast_timezone = tz
    end
  
    # Convert the given object into an object of <tt>Sequel.datetime_class</tt> in the
    # +application_timezone+.  Used when typecasting values when assigning them
    # to model datetime attributes.
    def typecast_to_application_timestamp(v)
      convert_timestamp(v, Sequel.typecast_timezone)
    end

    private

    # Convert the given +DateTime+ to the given input_timezone, keeping the
    # same time and just modifying the timezone.
    def convert_input_datetime_no_offset(v, input_timezone)
      case input_timezone
      when :utc, nil
        v # DateTime assumes UTC if no offset is given
      when :local
        offset = local_offset_for_datetime(v)
        v.new_offset(offset) - offset
      else
        convert_input_datetime_other(v, input_timezone)
      end
    end
    
    # Convert the given +DateTime+ to the given input_timezone that is not supported
    # by default (i.e. one other than +nil+, <tt>:local</tt>, or <tt>:utc</tt>).  Raises an +InvalidValue+ by default.
    # Can be overridden in extensions.
    def convert_input_datetime_other(v, input_timezone)
      raise InvalidValue, "Invalid input_timezone: #{input_timezone.inspect}"
    end
    
    # Converts the object from a +String+, +Array+, +Date+, +DateTime+, or +Time+ into an
    # instance of <tt>Sequel.datetime_class</tt>.  If given an array or a string that doesn't
    # contain an offset, assume that the array/string is already in the given +input_timezone+.
    def convert_input_timestamp(v, input_timezone)
      case v
      when String
        v2 = Sequel.string_to_datetime(v)
        if !input_timezone || Date._parse(v).has_key?(:offset)
          v2
        else
          # Correct for potentially wrong offset if string doesn't include offset
          if v2.is_a?(DateTime)
            v2 = convert_input_datetime_no_offset(v2, input_timezone)
          else
            # Time assumes local time if no offset is given
            v2 = v2.getutc + v2.utc_offset if input_timezone == :utc
          end
          v2
        end
      when Array
        y, mo, d, h, mi, s, ns, off = v
        if datetime_class == DateTime
          s += (defined?(Rational) ? Rational(ns, 1000000000) : ns/1000000000.0) if ns
          if off
            DateTime.civil(y, mo, d, h, mi, s, off)
          else
            convert_input_datetime_no_offset(DateTime.civil(y, mo, d, h, mi, s), input_timezone)
          end
        else
          Time.send(input_timezone == :utc ? :utc : :local, y, mo, d, h, mi, s, (ns ? ns / 1000.0 : 0))
        end
      when Hash
        ary = [:year, :month, :day, :hour, :minute, :second, :nanos].map{|x| (v[x] || v[x.to_s]).to_i}
        if (offset = (v[:offset] || v['offset']))
          ary << offset
        end
        convert_input_timestamp(ary, input_timezone)
        convert_input_timestamp(ary, input_timezone)
      when Time
        if datetime_class == DateTime
          if v.respond_to?(:to_datetime)
            v.to_datetime
          else
          # :nocov:
            # Ruby 1.8 code, %N not available and %z broken on Windows
            offset_hours, offset_minutes = (v.utc_offset/60).divmod(60)
            string_to_datetime(v.strftime("%Y-%m-%dT%H:%M:%S") << sprintf(".%06i%+03i%02i", v.usec, offset_hours, offset_minutes))
          # :nocov:
          end
        else
          v
        end
      when DateTime
        if datetime_class == DateTime
          v
        elsif v.respond_to?(:to_time)
          v.to_time
        else
        # :nocov:
          string_to_datetime(v.strftime("%FT%T.%N%z"))
        # :nocov:
        end
      else
        raise InvalidValue, "Invalid convert_input_timestamp type: #{v.inspect}"
      end
    end

    # Convert the given +DateTime+ to the given output_timezone that is not supported
    # by default (i.e. one other than +nil+, <tt>:local</tt>, or <tt>:utc</tt>).  Raises an +InvalidValue+ by default.
    # Can be overridden in extensions.
    def convert_output_datetime_other(v, output_timezone)
      raise InvalidValue, "Invalid output_timezone: #{output_timezone.inspect}"
    end
    
    # Convert the timezone setter argument.  Returns argument given by default,
    # exists for easier overriding in extensions.
    def convert_timezone_setter_arg(tz)
      tz
    end

    # Takes a DateTime dt, and returns the correct local offset for that dt, daylight savings included.
    def local_offset_for_datetime(dt)
      time_offset_to_datetime_offset Time.local(dt.year, dt.month, dt.day, dt.hour, dt.min, dt.sec).utc_offset
    end

    # Caches offset conversions to avoid excess Rational math.
    def time_offset_to_datetime_offset(offset_secs)
      @local_offsets ||= {}
      @local_offsets[offset_secs] ||= respond_to?(:Rational, true) ? Rational(offset_secs, 60*60*24) : offset_secs/60/60/24.0
    end
  end

  extend Timezones
end
