# The pg_inet extension adds support for Sequel to handle
# PostgreSQL's inet and cidr types using ruby's IPAddr class.
#
# This extension integrates with Sequel's native postgres adapter, so
# that when inet/cidr fields are retrieved, they are returned as
# IPAddr instances
#
# After loading the extension, you should extend your dataset
# with a module so that it correctly handles the inet/cidr type:
#
#   DB.extension :pg_inet
#
# If you are not using the native postgres adapter, you probably
# also want to use the pg_typecast_on_load plugin in the model, and
# set it to typecast the inet/cidr column(s) on load.
#
# This extension integrates with the pg_array extension.  If you plan
# to use the inet[] or cidr[] types, load the pg_array extension before
# the pg_inet extension:
#
#   DB.extension :pg_array, :pg_inet
#
# This extension does not add special support for the macaddr
# type.  Ruby doesn't have a stdlib class that represents mac
# addresses, so these will still be returned as strings.  The exception
# to this is that the pg_array extension integration will recognize
# macaddr[] types return them as arrays of strings.

require 'ipaddr'
Sequel.require 'adapters/utils/pg_types'

module Sequel
  module Postgres
    # Methods enabling Database object integration with the inet/cidr types.
    module InetDatabaseMethods
      # Reset the conversion procs when extending the Database object, so
      # it will pick up the inet/cidr converter.  Also, extend the datasets
      # with support for literalizing the IPAddr types.
      def self.extended(db)
        db.extend_datasets(InetDatasetMethods)
        db.send(:copy_conversion_procs, [869, 650, 1041, 651, 1040])
      end

      # Convert an IPAddr arg to a string.  Probably not necessary, but done
      # for safety.
      def bound_variable_arg(arg, conn)
        case arg
        when IPAddr
          "#{arg.to_s}/#{arg.instance_variable_get(:@mask_addr).to_s(2).count('1')}"
        else
          super
        end
      end

      # Make the column type detection recognize the inet and cidr types.
      def schema_column_type(db_type)
        case db_type
        when 'inet', 'cidr'
          :ipaddr
        else
          super
        end
      end

      private

      # Handle inet[]/cidr[] types in bound variables.
      def bound_variable_array(a)
        case a
        when IPAddr
          "\"#{a.to_s}/#{a.instance_variable_get(:@mask_addr).to_s(2).count('1')}\""
        else
          super
        end
      end

      # Typecast the given value to an IPAddr object.
      def typecast_value_ipaddr(value)
        case value
        when IPAddr
          value
        when String
          IPAddr.new(value)
        else
          raise Sequel::InvalidValue, "invalid value for inet/cidr: #{value.inspect}"
        end
      end
    end

    module InetDatasetMethods
      private

      # Convert IPAddr value to a string and append a literal version
      # of the string to the sql.
      def literal_other_append(sql, value)
        if value.is_a?(IPAddr)
          literal_string_append(sql, "#{value.to_s}/#{value.instance_variable_get(:@mask_addr).to_s(2).count('1')}")
        else
          super
        end
      end
    end

    PG_TYPES[869] = PG_TYPES[650] = IPAddr.method(:new)
    if defined?(PGArray) && PGArray.respond_to?(:register)
      PGArray.register('inet', :oid=>1041, :scalar_oid=>869)
      PGArray.register('cidr', :oid=>651, :scalar_oid=>650)
      PGArray.register('macaddr', :oid=>1040)
    end
  end

  Database.register_extension(:pg_inet, Postgres::InetDatabaseMethods)
end
