# frozen-string-literal: true
#
# The pg_timestamptz extension changes the default timestamp
# type for the database to be +timestamptz+ (<tt>timestamp with time zone</tt>)
# instead of +timestamp+ (<tt>timestamp without time zone</tt>).  This is
# recommended if you are dealing with multiple timezones in your application.
#
# If you are using the auto_cast_date_and_time extension, the pg_timestamptz
# extension will automatically cast Time and DateTime values to
# <tt>TIMESTAMP WITH TIME ZONE</tt> instead of +TIMESTAMP+.
# 
# To load the extension into the database:
#
#   DB.extension :pg_timestamptz
#
# To load the extension into individual datasets:
#
#   ds = ds.extension(:pg_timestamptz)
#
# Note that the loading into individual datasets only affects the integration
# with the auto_cast_date_and_time extension.
#
# Related modules: Sequel::Postgres::Timestamptz, Sequel::Postgres::TimestamptzDatasetMethods

#
module Sequel
  module Postgres
    module Timestamptz
      def self.extended(db)
        db.extend_datasets(TimestamptzDatasetMethods)
      end

      private

      # Use timestamptz by default for generic timestamp value.
      def type_literal_generic_datetime(column)
        :timestamptz
      end
    end

    module TimestamptzDatasetMethods
      private

      def literal_datetime_timestamp_cast
        'TIMESTAMP WITH TIME ZONE '
      end
    end
  end

  Dataset.register_extension(:pg_timestamptz, Postgres::TimestamptzDatasetMethods)
  Database.register_extension(:pg_timestamptz, Postgres::Timestamptz)
end
