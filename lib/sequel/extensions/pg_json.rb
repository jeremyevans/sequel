# The pg_json extension adds support for Sequel to handle
# PostgreSQL's json type.  It is slightly more strict than the
# PostgreSQL json type in that the object returned must be an
# array or object (PostgreSQL's json type considers plain numbers
# and strings as valid).  This is because Sequel relies completely
# on the ruby JSON library for parsing, and ruby's JSON library
# does not accept the values.
#
# This extension integrates with Sequel's native postgres adapter, so
# that when json fields are retrieved, they are parsed and returned
# as instances of Sequel::Postgres::JSONArray or
# Sequel::Postgres::JSONHash.  JSONArray and JSONHash are
# DelegateClasses of Array and Hash, so they mostly act the same, but
# not completely (json_array.is_a?(Array) is false).  If you want
# the actual array for a JSONArray, call JSONArray#to_a.  If you want
# the actual hash for a JSONHash, call JSONHash#to_hash.
# This is done so that Sequel does not treat JSONArray and JSONHash
# like Array and Hash by default, which would cause issues.
#
# To turn an existing Array or Hash into a JSONArray or JSONHash:
#
#   array.pg_json
#   hash.pg_json
#
# So if you want to insert an array or hash into an json database column:
#
#   DB[:table].insert(:column=>[1, 2, 3].pg_json)
#   DB[:table].insert(:column=>{'a'=>1, 'b'=>2}.pg_json)
#
# If you would like to use PostgreSQL json columns in your model
# objects, you probably want to modify the schema parsing/typecasting
# so that it recognizes and correctly handles the json type, which
# you can do by:
#
#   DB.extension :pg_json
#
# If you are not using the native postgres adapter, you probably
# also want to use the typecast_on_load plugin in the model, and
# set it to typecast the json column(s) on load.
#
# This extension integrates with the pg_array extension.  If you plan
# to use the json[] type, load the pg_array extension before the
# pg_json extension:
#
#   DB.extension :pg_array, :pg_json
#
# This extension requires both the json and delegate libraries.

require 'delegate'
require 'json'
Sequel.require 'adapters/utils/pg_types'

module Sequel
  module Postgres
    CAST_JSON = '::json'.freeze

    # Class representating PostgreSQL JSON column array values.
    class JSONArray < DelegateClass(Array)
      # Convert the array to a string using to_json, append a
      # literalized version of the string to the sql, and explicitly
      # cast the string to json.
      def sql_literal_append(ds, sql)
        ds.literal_append(sql, to_json)
        sql << CAST_JSON
      end
    end

    # Class representating PostgreSQL JSON column hash/object values.
    class JSONHash < DelegateClass(Hash)
      # Convert the array to a string using to_json, append a
      # literalized version of the string to the sql, and explicitly
      # cast the string to json.
      def sql_literal_append(ds, sql)
        ds.literal_append(sql, to_json)
        sql << CAST_JSON
      end

      # Return the object being delegated to.
      alias to_hash __getobj__
    end

    # Methods enabling Database object integration with the json type.
    module JSONDatabaseMethods
      # Parse the given string as json, returning either a JSONArray
      # or JSONHash instance, and raising an error if the JSON
      # parsing does not yield an array or hash.
      def self.parse_json(s)
        begin
          value = JSON.parse(s)
        rescue JSON::ParserError=>e
          raise Sequel.convert_exception_class(e, Sequel::InvalidValue)
        end

        case value
        when Array
          JSONArray.new(value)
        when Hash
          JSONHash.new(value)
        else
          raise Sequel::InvalidValue, "unhandled json value: #{value.inspect} (from #{s.inspect})"
        end
      end

      # Reset the conversion procs when extending the Database object, so
      # it will pick up the json convertor.  This is only done for the native
      # postgres adapter.
      def self.extended(db)
        db.reset_conversion_procs if db.respond_to?(:reset_conversion_procs)
      end

      # Handle JSONArray and JSONHash in bound variables
      def bound_variable_arg(arg, conn)
        case arg
        when JSONArray, JSONHash
          arg.to_json
        else
          super
        end
      end

      # Make the column type detection recognize the json type.
      def schema_column_type(db_type)
        case db_type
        when 'json'
          :json
        else
          super
        end
      end

      private

      # Handle json[] types in bound variables.
      def bound_variable_array(a)
        case a
        when JSONHash, JSONArray
          "\"#{a.to_json.gsub('"', '\\"')}\""
        else
          super
        end
      end

      # Given a value to typecast to the json column
      # * If given a JSONArray or JSONHash, just return the value
      # * If given an Array, return a JSONArray
      # * If given a Hash, return a JSONHash
      # * If given a String, parse it as would be done during
      #   database retrieval.
      def typecast_value_json(value)
        case value
        when JSONArray, JSONHash
          value
        when Array
          JSONArray.new(value)
        when Hash
          JSONHash.new(value)
        when String
          JSONDatabaseMethods.parse_json(value)
        else
          raise Sequel::InvalidValue, "invalid value for json: #{value.inspect}"
        end
      end
    end

    PG_TYPES[114] = JSONDatabaseMethods.method(:parse_json)
    if defined?(PGArray) && PGArray.respond_to?(:register)
      PGArray.register('json', :oid=>199, :scalar_oid=>114)
    end
  end

  Database.register_extension(:pg_json, Postgres::JSONDatabaseMethods)
end

class Array
  # Return a Sequel::Postgres::JSONArray proxy to the receiver.
  # This is mostly useful as a short cut for creating JSONArray
  # objects that didn't come from the database.
  def pg_json
    Sequel::Postgres::JSONArray.new(self)
  end
end

class Hash
  # Return a Sequel::Postgres::JSONHash proxy to the receiver.
  # This is mostly useful as a short cut for creating JSONHash
  # objects that didn't come from the database.
  def pg_json
    Sequel::Postgres::JSONHash.new(self)
  end
end
