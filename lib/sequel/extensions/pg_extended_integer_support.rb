# frozen-string-literal: true
#
# The pg_extended_integer_support extension supports literalizing
# Ruby integers outside of PostgreSQL bigint range on PostgreSQL.
# Sequel by default will raise exceptions when
# literalizing such integers, as PostgreSQL would treat them
# as numeric type values instead of integer/bigint type values
# if unquoted, which can result in unexpected negative performance
# (e.g. forcing sequential scans when index scans would be used for
# an integer/bigint type).
#
# To load the extension into a Dataset (this returns a new Dataset):
#
#   dataset = dataset.extension(:pg_extended_integer_support)
#
# To load the extension into a Database, so it affects all of the
# Database's datasets:
#
#   DB.extension :pg_extended_integer_support
#
# By default, the extension will quote integers outside
# bigint range:
#
#   DB.literal(2**63) # => "'9223372036854775808'"
#
# Quoting the value treats the type as unknown:
#
#   DB.get{pg_typeof(2**63)} # => 'unknown'
#
# PostgreSQL will implicitly cast the unknown type to the appropriate
# database type, raising an error if it cannot be casted. Be aware this
# can result in the integer value being implicitly casted to text or
# any other PostgreSQL type:
#
#   # Returns a string, not an integer:
#   DB.get{2**63}
#   # => "9223372036854775808"
#
# You can use the Dataset#integer_outside_bigint_range_strategy method
# with the value +:raw+ to change the strategy to not quote the variable:
#
#  DB.dataset.
#    integer_outside_bigint_range_strategy(:raw).
#    literal(2**63)
#  # => "9223372036854775808"
#
# Note that not quoting the value will result in PostgreSQL treating
# the type as numeric instead of integer:
#
#  DB.dataset.
#    integer_outside_bigint_range_strategy(:raw).
#    get{pg_typeof(2**63)}
#  # => "numeric"
#
# The +:raw+ behavior was Sequel's historical behavior, but unless
# you fully understand the reprecussions of PostgreSQL using a
# numeric type for integer values, you should not use it.
#
# To get the current default behavior of raising an exception for
# integers outside of PostgreSQL bigint range, you can use a strategy
# of +:raise+.
#
# To specify a default strategy for handling integers outside
# bigint range that applies to all of a Database's datasets, you can
# use the +:integer_outside_bigint_range_strategy+ Database option with
# a value of +:raise+ or +:raw+:
#
#   DB.opts[:integer_outside_bigint_range_strategy] = :raw
#
# The Database option will be used as a fallback if you did not call
# the Dataset#integer_outside_bigint_range_strategy method to specify
# a strategy for the dataset.
#
# Related module: Sequel::Postgres::ExtendedIntegerSupport

#
module Sequel
  module Postgres
    module ExtendedIntegerSupport
      # Set the strategy for handling integers outside PostgreSQL
      # bigint range.  Supported values:
      #
      # :quote :: Quote the integer value. PostgreSQL will treat
      #           the integer as a unknown type, implicitly casting
      #           to any other type as needed. This is the default
      #           value when using the pg_extended_integer_support
      #           extension.
      # :raise :: Raise error when attempting to literalize the integer
      #           (the default behavior of Sequel on PostgreSQL when
      #           not using the pg_extended_integer_support extension).
      # :raw :: Use raw integer value without quoting. PostgreSQL
      #         will treat the integer as a numeric. This was Sequel's
      #         historical behavior, but it is unlikely to be desired.
      def integer_outside_bigint_range_strategy(strategy)
        clone(:integer_outside_bigint_range_strategy=>strategy)
      end

      private

      # Handle integers outside the bigint range by using
      # the configured strategy.
      def literal_integer_outside_bigint_range(v)
        case @opts[:integer_outside_bigint_range_strategy] || @db.opts[:integer_outside_bigint_range_strategy]
        when :raise
          super
        when :raw
          v.to_s
        else # when :quote
          "'#{v}'"
        end
      end
    end
  end

  Dataset.register_extension(:pg_extended_integer_support, Postgres::ExtendedIntegerSupport)
end
