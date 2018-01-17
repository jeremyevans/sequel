# frozen-string-literal: true
#
# The pg_json extension adds support for Sequel to handle
# PostgreSQL's json and jsonb types.  It is slightly more strict than the
# PostgreSQL json types in that the object returned should be an
# array or object (PostgreSQL's json type considers plain numbers
# strings, true, false, and null as valid).  Sequel will work with
# PostgreSQL json values that are not arrays or objects, but support
# is fairly limited and the values do not roundtrip.
#
# This extension integrates with Sequel's native postgres and jdbc/postgresql adapters, so
# that when json fields are retrieved, they are parsed and returned
# as instances of Sequel::Postgres::JSONArray or
# Sequel::Postgres::JSONHash (or JSONBArray or JSONBHash for jsonb
# columns).  JSONArray and JSONHash are
# DelegateClasses of Array and Hash, so they mostly act the same, but
# not completely (json_array.is_a?(Array) is false).  If you want
# the actual array for a JSONArray, call JSONArray#to_a.  If you want
# the actual hash for a JSONHash, call JSONHash#to_hash.
# This is done so that Sequel does not treat JSONArray and JSONHash
# like Array and Hash by default, which would cause issues.
#
# To turn an existing Array or Hash into a JSONArray or JSONHash,
# use Sequel.pg_json:
#
#   Sequel.pg_json(array) # or Sequel.pg_jsonb(array) for jsonb type
#   Sequel.pg_json(hash)  # or Sequel.pg_jsonb(hash) for jsonb type
#
# If you have loaded the {core_extensions extension}[rdoc-ref:doc/core_extensions.rdoc],
# or you have loaded the core_refinements extension
# and have activated refinements for the file, you can also use Array#pg_json and Hash#pg_json:
#
#   array.pg_json # or array.pg_jsonb for jsonb type
#   hash.pg_json  # or hash.pg_jsonb for jsonb type
#
# So if you want to insert an array or hash into an json database column:
#
#   DB[:table].insert(column: Sequel.pg_json([1, 2, 3]))
#   DB[:table].insert(column: Sequel.pg_json({'a'=>1, 'b'=>2}))
#
# To use this extension, please load it into the Database instance:
#
#   DB.extension :pg_json
#
# See the {schema modification guide}[rdoc-ref:doc/schema_modification.rdoc]
# for details on using json columns in CREATE/ALTER TABLE statements.
#
# This extension integrates with the pg_array extension.  If you plan
# to use the json[] type, load the pg_array extension before the
# pg_json extension:
#
#   DB.extension :pg_array, :pg_json
#
# Note that when accessing json hashes, you should always use strings for keys.
# Attempting to use other values (such as symbols) will not work correctly.
#
# This extension requires both the json and delegate libraries.
#
# Related modules: Sequel::Postgres::JSONArrayBase, Sequel::Postgres::JSONArray,
# Sequel::Postgres::JSONArray, Sequel::Postgres::JSONBArray, Sequel::Postgres::JSONHashBase,
# Sequel::Postgres::JSONHash, Sequel::Postgres::JSONBHash, Sequel::Postgres::JSONDatabaseMethods

require 'delegate'
require 'json'

module Sequel
  module Postgres
    # Class representing PostgreSQL JSON/JSONB column array values.
    class JSONArrayBase < DelegateClass(Array)
      include Sequel::SQL::AliasMethods
      include Sequel::SQL::CastMethods

      # Convert the array to a json string and append a
      # literalized version of the string to the sql.
      def sql_literal_append(ds, sql)
        ds.literal_append(sql, Sequel.object_to_json(self))
      end
    end

    class JSONArray < JSONArrayBase
      # Cast as json
      def sql_literal_append(ds, sql)
        super
        sql << '::json'
      end
    end

    class JSONBArray < JSONArrayBase
      # Cast as jsonb
      def sql_literal_append(ds, sql)
        super
        sql << '::jsonb'
      end
    end

    # Class representing PostgreSQL JSON/JSONB column hash/object values.
    class JSONHashBase < DelegateClass(Hash)
      include Sequel::SQL::AliasMethods
      include Sequel::SQL::CastMethods

      # Convert the hash to a json string and append a
      # literalized version of the string to the sql.
      def sql_literal_append(ds, sql)
        ds.literal_append(sql, Sequel.object_to_json(self))
      end

      # Return the object being delegated to.
      alias to_hash __getobj__
    end

    class JSONHash < JSONHashBase
      # Cast as json
      def sql_literal_append(ds, sql)
        super
        sql << '::json'
      end
    end

    class JSONBHash < JSONHashBase
      # Cast as jsonb
      def sql_literal_append(ds, sql)
        super
        sql << '::jsonb'
      end
    end

    # Methods enabling Database object integration with the json type.
    module JSONDatabaseMethods
      def self.extended(db)
        db.instance_exec do
          add_conversion_proc(114, JSONDatabaseMethods.method(:db_parse_json))
          add_conversion_proc(3802, JSONDatabaseMethods.method(:db_parse_jsonb))
          if respond_to?(:register_array_type)
            register_array_type('json', :oid=>199, :scalar_oid=>114)
            register_array_type('jsonb', :oid=>3807, :scalar_oid=>3802)
          end
          @schema_type_classes[:json] = [JSONHash, JSONArray]
          @schema_type_classes[:jsonb] = [JSONBHash, JSONBArray]
        end
      end

      # Parse JSON data coming from the database.  Since PostgreSQL allows
      # non JSON data in JSON fields (such as plain numbers and strings),
      # we don't want to raise an exception for that.
      def self.db_parse_json(s)
        parse_json(s)
      rescue Sequel::InvalidValue
        raise unless s.is_a?(String)
        parse_json("[#{s}]").first
      end

      # Same as db_parse_json, but consider the input as jsonb.
      def self.db_parse_jsonb(s)
        parse_json(s, true)
      rescue Sequel::InvalidValue
        raise unless s.is_a?(String)
        parse_json("[#{s}]").first
      end

      # Parse the given string as json, returning either a JSONArray
      # or JSONHash instance (or JSONBArray or JSONBHash instance if jsonb
      # argument is true), or a String, Numeric, true, false, or nil
      # if the json library used supports that.
      def self.parse_json(s, jsonb=false)
        begin
          value = Sequel.parse_json(s)
        rescue Sequel.json_parser_error_class => e
          raise Sequel.convert_exception_class(e, Sequel::InvalidValue)
        end

        case value
        when Array
          (jsonb ? JSONBArray : JSONArray).new(value)
        when Hash 
          (jsonb ? JSONBHash : JSONHash).new(value)
        when String, Numeric, true, false, nil
          value
        else
          raise Sequel::InvalidValue, "unhandled json value: #{value.inspect} (from #{s.inspect})"
        end
      end

      # Handle json and jsonb types in bound variables
      def bound_variable_arg(arg, conn)
        case arg
        when JSONArrayBase, JSONHashBase
          Sequel.object_to_json(arg)
        else
          super
        end
      end

      private

      # Handle json[] and jsonb[] types in bound variables.
      def bound_variable_array(a)
        case a
        when JSONHashBase, JSONArrayBase
          "\"#{Sequel.object_to_json(a).gsub('"', '\\"')}\""
        else
          super
        end
      end

      # Make the column type detection recognize the json types.
      def schema_column_type(db_type)
        case db_type
        when 'json'
          :json
        when 'jsonb'
          :jsonb
        else
          super
        end
      end

      # Set the :callable_default value if the default value is recognized as an empty json/jsonb array/hash.
      def schema_parse_table(*)
        super.each do |a|
          h = a[1]
          if (h[:type] == :json || h[:type] == :jsonb) && h[:default] =~ /\A'(\{\}|\[\])'::jsonb?\z/
            is_array = $1 == '[]'

            klass = if h[:type] == :json
              if is_array
                JSONArray
              else
                JSONHash
              end
            elsif is_array
              JSONBArray
            else
              JSONBHash
            end

            h[:callable_default] = lambda{klass.new(is_array ? [] : {})}
          end
        end
      end

      # Convert the value given to a JSONArray or JSONHash
      def typecast_value_json(value)
        case value
        when JSONArray, JSONHash
          value
        when Array
          JSONArray.new(value)
        when Hash 
          JSONHash.new(value)
        when JSONBArray
          JSONArray.new(value.to_a)
        when JSONBHash
          JSONHash.new(value.to_hash)
        when String
          JSONDatabaseMethods.parse_json(value)
        else
          raise Sequel::InvalidValue, "invalid value for json: #{value.inspect}"
        end
      end

      # Convert the value given to a JSONBArray or JSONBHash
      def typecast_value_jsonb(value)
        case value
        when JSONBArray, JSONBHash
          value
        when Array
          JSONBArray.new(value)
        when Hash 
          JSONBHash.new(value)
        when JSONArray
          JSONBArray.new(value.to_a)
        when JSONHash
          JSONBHash.new(value.to_hash)
        when String
          JSONDatabaseMethods.parse_json(value, true)
        else
          raise Sequel::InvalidValue, "invalid value for jsonb: #{value.inspect}"
        end
      end
    end
  end

  module SQL::Builders
    # Wrap the array or hash in a Postgres::JSONArray or Postgres::JSONHash.
    def pg_json(v)
      case v
      when Postgres::JSONArray, Postgres::JSONHash
        v
      when Array
        Postgres::JSONArray.new(v)
      when Hash
        Postgres::JSONHash.new(v)
      when Postgres::JSONBArray
        Postgres::JSONArray.new(v.to_a)
      when Postgres::JSONBHash
        Postgres::JSONHash.new(v.to_hash)
      else
        Sequel.pg_json_op(v)
      end
    end

    # Wrap the array or hash in a Postgres::JSONBArray or Postgres::JSONBHash.
    def pg_jsonb(v)
      case v
      when Postgres::JSONBArray, Postgres::JSONBHash
        v
      when Array
        Postgres::JSONBArray.new(v)
      when Hash
        Postgres::JSONBHash.new(v)
      when Postgres::JSONArray
        Postgres::JSONBArray.new(v.to_a)
      when Postgres::JSONHash
        Postgres::JSONBHash.new(v.to_hash)
      else
        Sequel.pg_jsonb_op(v)
      end
    end
  end

  Database.register_extension(:pg_json, Postgres::JSONDatabaseMethods)
end

# :nocov:
if Sequel.core_extensions?
  class Array
    # Return a Sequel::Postgres::JSONArray proxy to the receiver.
    # This is mostly useful as a short cut for creating JSONArray
    # objects that didn't come from the database.
    def pg_json
      Sequel::Postgres::JSONArray.new(self)
    end

    # Return a Sequel::Postgres::JSONArray proxy to the receiver.
    # This is mostly useful as a short cut for creating JSONArray
    # objects that didn't come from the database.
    def pg_jsonb
      Sequel::Postgres::JSONBArray.new(self)
    end
  end

  class Hash
    # Return a Sequel::Postgres::JSONHash proxy to the receiver.
    # This is mostly useful as a short cut for creating JSONHash
    # objects that didn't come from the database.
    def pg_json
      Sequel::Postgres::JSONHash.new(self)
    end

    # Return a Sequel::Postgres::JSONHash proxy to the receiver.
    # This is mostly useful as a short cut for creating JSONHash
    # objects that didn't come from the database.
    def pg_jsonb
      Sequel::Postgres::JSONBHash.new(self)
    end
  end
end

if defined?(Sequel::CoreRefinements)
  module Sequel::CoreRefinements
    refine Array do
      def pg_json
        Sequel::Postgres::JSONArray.new(self)
      end

      def pg_jsonb
        Sequel::Postgres::JSONBArray.new(self)
      end
    end

    refine Hash do
      def pg_json
        Sequel::Postgres::JSONHash.new(self)
      end

      def pg_jsonb
        Sequel::Postgres::JSONBHash.new(self)
      end
    end
  end
end
# :nocov:
