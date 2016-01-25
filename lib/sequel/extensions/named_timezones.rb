# frozen-string-literal: true
#
# Allows the use of named timezones via TZInfo (requires tzinfo).
# Forces the use of DateTime as Sequel's datetime_class, since
# ruby's Time class doesn't support timezones other than local
# and UTC.
#
# This allows you to either pass strings or TZInfo::Timezone
# instance to Sequel.database_timezone=, application_timezone=, and
# typecast_timezone=.  If a string is passed, it is converted to a
# TZInfo::Timezone using TZInfo::Timezone.get.
#
# To load the extension:
#
#   Sequel.extension :named_timezones
#
# Let's say you have the database server in New York and the
# application server in Los Angeles.  For historical reasons, data
# is stored in local New York time, but the application server only
# services clients in Los Angeles, so you want to use New York
# time in the database and Los Angeles time in the application.  This
# is easily done via:
#
#   Sequel.database_timezone = 'America/New_York'
#   Sequel.application_timezone = 'America/Los_Angeles'
#
# Then, before data is stored in the database, it is converted to New
# York time.  When data is retrieved from the database, it is
# converted to Los Angeles time.
#
# If you are using database specific timezones, you may want to load
# this extension into the database in order to support similar API:
#
#   DB.extension :named_timezones
#   DB.timezone = 'America/New_York'
#
# Note that typecasting from the database timezone to the application
# timezone when fetching rows is dependent on the database adapter,
# and only works on adapters where Sequel itself does the conversion.
# It should work on mysql, postgres, sqlite, ibmdb, and jdbc.

require 'tzinfo'

#
module Sequel
  self.datetime_class = DateTime
  
  module NamedTimezones
    module DatabaseMethods
      def timezone=(tz)
        super(Sequel.send(:convert_timezone_setter_arg, tz))
      end
    end

    # Handles TZInfo::AmbiguousTime exceptions automatically by providing a
    # proc called with both the datetime value being converted as well as
    # the array of TZInfo::TimezonePeriod results. Example:
    #
    #   Sequel.tzinfo_disambiguator = proc{|datetime, periods| periods.first}
    attr_accessor :tzinfo_disambiguator

    private 
    
    # Assume the given DateTime has a correct time but a wrong timezone.  It is
    # currently in UTC timezone, but it should be converted to the input_timezone.
    # Keep the time the same but convert the timezone to the input_timezone.
    # Expects the input_timezone to be a TZInfo::Timezone instance.
    def convert_input_datetime_other(v, input_timezone)
      local_offset = input_timezone.period_for_local(v, &tzinfo_disambiguator_for(v)).utc_total_offset_rational
      (v - local_offset).new_offset(local_offset)
    end

    # Convert the given DateTime to use the given output_timezone.
    # Expects the output_timezone to be a TZInfo::Timezone instance.
    def convert_output_datetime_other(v, output_timezone)
      # TZInfo converts times, but expects the given DateTime to have an offset
      # of 0 and always leaves the timezone offset as 0
      v = output_timezone.utc_to_local(v.new_offset(0))
      local_offset = output_timezone.period_for_local(v, &tzinfo_disambiguator_for(v)).utc_total_offset_rational
      # Convert timezone offset from UTC to the offset for the output_timezone
      (v - local_offset).new_offset(local_offset)
    end
    
    # Returns TZInfo::Timezone instance if given a String.
    def convert_timezone_setter_arg(tz)
      tz.is_a?(String) ? TZInfo::Timezone.get(tz) : super
    end

    # Return a disambiguation proc that provides both the datetime value
    # and the periods, in order to allow the choice of period to depend
    # on the datetime value.
    def tzinfo_disambiguator_for(v)
      if pr = @tzinfo_disambiguator
        proc{|periods| pr.call(v, periods)}
      end
    end
  end
  
  extend NamedTimezones
  Database.register_extension(:named_timezones, NamedTimezones::DatabaseMethods)
end
