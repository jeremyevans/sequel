# frozen-string-literal: true
#
# The pg_auto_parameterize_in_array extension builds on the pg_auto_parameterize
# extension, adding support for handling additional types when converting from
# IN to = ANY and NOT IN to != ALL:
#
#   DB[:table].where(column: [1.0, 2.0, ...])
#   # Without extension: column IN ($1::numeric, $2:numeric, ...) # bound variables: 1.0, 2.0, ...
#   # With extension:    column = ANY($1::numeric[]) # bound variables: [1.0, 2.0, ...]
#
# This prevents the use of an unbounded number of bound variables based on the
# size of the array, as well as using different SQL for different array sizes.
#
# The following types are supported when doing the conversions, with the database
# type used:
#
# Float :: if any are infinite or NaN, double precision, otherwise numeric
# BigDecimal :: numeric
# Date :: date
# Time :: timestamp (or timestamptz if pg_timestamptz extension is used)
# DateTime :: timestamp (or timestamptz if pg_timestamptz extension is used)
# Sequel::SQLTime :: time
# Sequel::SQL::Blob :: bytea
#
# Arrays of string values are not automatically converted by default, because the Ruby
# String class can represent a number of different database types.  To convert
# arrays of Ruby strings to an untyped array (a query parameter with no explicit
# type cast), set the +:treat_string_list_as_untyped_array+ Database option
# before loading the extension.
# 
# If you will only be using arrays of Ruby strings that represent the +text+ type,
# you can use the +:treat_string_list_as_text_array+ Database option is used. This
# can break programs, since the type for literal strings in PostgreSQL is +unknown+,
# not +text+.
#
# The conversion is only done for single dimensional arrays that have two or
# more elements, where all elements are of the same class (other than
# +nil+ values).  You can also do the conversion for arrays of 1 element by setting
# <tt>pg_auto_parameterize_min_array_size: 1</tt> Database option.  This makes
# finding cases that need special handling easier, but it doesn't match
# how PostgreSQL internally converts the expression (PostgreSQL converts
# <tt>IN (single_value)</tt> to <tt>= single_value</tt>, not
# <tt>= ANY(ARRAY[single_value])</tt>).
#
# Related module: Sequel::Postgres::AutoParameterizeInArray

module Sequel
  module Postgres
    # Enable automatically parameterizing queries.
    module AutoParameterizeInArray
      module TreatStringListAsUntypedArray
        # Sentinal value to use as an auto param type to use auto parameterization
        # of a string array without an explicit type cast.
        NO_EXPLICIT_CAST = Object.new.freeze

        # Wrapper for untyped PGArray values that will be parameterized directly
        # into the query.  This should only be used in cases where you know the
        # value should be added as a query parameter.
        class ParameterizedUntypedPGArray < SQL::Wrapper
          def to_s_append(ds, sql)
            sql.add_arg(@value)
          end
        end

        private

        # Recognize NO_EXPLICIT_CAST sentinal value and use wrapped
        # PGArray that will be parameterized into the query.
        def _convert_array_to_pg_array_with_type(r, type)
          if NO_EXPLICIT_CAST.equal?(type)
            ParameterizedUntypedPGArray.new(Sequel.pg_array(r))
          else
            super
          end
        end

        # Use a query parameter with no type cast for string arrays.
        def _bound_variable_type_for_string_array(r)
          NO_EXPLICIT_CAST
        end
      end

      module TreatStringListAsTextArray
        private

        # Assume all string arrays used on RHS of IN/NOT IN are for type text[]
        def _bound_variable_type_for_string_array(r)
          "text"
        end
      end

      # Transform column IN (...) expressions into column = ANY($)
      # and column NOT IN (...) expressions into column != ALL($)
      # using an array bound variable for the ANY/ALL argument,
      # if all values inside the predicate are of the same type and
      # the type is handled by the extension.
      # This is the same optimization PostgreSQL performs internally,
      # but this reduces the number of bound variables.
      def complex_expression_sql_append(sql, op, args)
        case op
        when :IN, :"NOT IN"
          l, r = args
          if auto_param?(sql) && (type = _bound_variable_type_for_array(r))
            if op == :IN 
              op = :"="
              func = :ANY
            else
              op = :!=
              func = :ALL
            end
            args = [l, Sequel.function(func, _convert_array_to_pg_array_with_type(r, type))]
          end
        end

        super
      end

      private

      # The bound variable type string to use for the bound variable array.
      # Returns nil if a bound variable should not be used for the array.
      def _bound_variable_type_for_array(r)
        return unless Array === r && r.size >= pg_auto_parameterize_min_array_size
        classes = r.map(&:class)
        classes.uniq!
        classes.delete(NilClass)
        return unless classes.size == 1

        klass = classes[0]
        if klass == Integer
          # This branch is not taken on Ruby <2.4, because of the Fixnum/Bignum split.
          # However, that causes no problems as pg_auto_parameterize handles integer
          # arrays natively (though the SQL used is different)
          "int8"
        elsif klass == String
          _bound_variable_type_for_string_array(r)
        elsif klass == BigDecimal
          "numeric"
        elsif klass == Date
          "date"
        elsif klass == Time
          @db.cast_type_literal(Time)
        elsif klass == Float
          # PostgreSQL treats literal floats as numeric, not double precision
          # But older versions of PostgreSQL don't handle Infinity/NaN in numeric
          r.all?{|v| v.nil? || v.finite?} ? "numeric" : "double precision"
        elsif klass == Sequel::SQLTime
          "time"
        elsif klass == DateTime
          @db.cast_type_literal(DateTime)
        elsif klass == Sequel::SQL::Blob
          "bytea"
        end
      end

      # Do not auto parameterize string arrays by default.
      def _bound_variable_type_for_string_array(r)
        nil
      end

      # The minimium size of array to auto parameterize.
      def pg_auto_parameterize_min_array_size
        2
      end

      # Convert RHS of IN/NOT IN operator to PGArray with given type.
      def _convert_array_to_pg_array_with_type(r, type)
        Sequel.pg_array(r, type)
      end
    end
  end

  Database.register_extension(:pg_auto_parameterize_in_array) do |db|
    db.extension(:pg_array, :pg_auto_parameterize)
    db.extend_datasets(Postgres::AutoParameterizeInArray)

    if db.typecast_value(:boolean, db.opts[:treat_string_list_as_text_array])
      db.extend_datasets(Postgres::AutoParameterizeInArray::TreatStringListAsTextArray)
    elsif db.typecast_value(:boolean, db.opts[:treat_string_list_as_untyped_array])
      db.extend_datasets(Postgres::AutoParameterizeInArray::TreatStringListAsUntypedArray)
    end

    if min_array_size = db.opts[:pg_auto_parameterize_min_array_size]
      min_array_size = db.typecast_value(:integer, min_array_size)
      mod = Module.new do
        define_method(:pg_auto_parameterize_min_array_size){min_array_size}
        private :pg_auto_parameterize_min_array_size
      end
      Sequel.set_temp_name(mod){"Sequel::Postgres::AutoParameterizeInArray::_MinArraySize#{min_array_size}"}
      db.extend_datasets(mod)
    end
  end
end
