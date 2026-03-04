# frozen-string-literal: true
#
# The lit_require_frozen extension disallows the use of unfrozen strings
# as literal strings in database and dataset methods. If you try to use an
# unfrozen string as a literal string for a dataset using this extension,
# an exception will be raised.
#
# While this works for all Ruby versions, it is designed for use on Ruby 3+
# where all files are using the frozen-string-literal magic comment. In this
# case, uninterpolated literal strings are frozen, but interpolated strings
# are not frozen. This allows you to catch potentially dangerous code:
#
#   # Probably safe, no exception raised
#   DB["SELECT * FROM t WHERE c > :v", v: user_provided_string)
#
#   # Potentially unsafe, raises Sequel::LitRequireFrozen::Error
#   DB["SELECT * FROM t WHERE c > '#{user_provided_string}'"]
#
# The assumption made is that a frozen string is unlikely to contain unsafe
# input, while an unfrozen string has potentially been interpolated and may
# contain unsafe input.
#
# This disallows the the following cases:
#
# * Sequel::LiteralString instances that are unfrozen and are not based on a
#   frozen string
# * Sequel::SQL::PlaceholderLiteralString instances when the placeholder string
#   is not frozen
# * Unfrozen strings passed to Database#<< or #[] or Dataset#with_sql
#
# To use this extension, load it into the database:
#
#   DB.extension :lit_require_frozen
#
# It can also be loaded into individual datasets:
#
#  ds = DB[:t].extension(:lit_require_frozen)
#
# Assuming you have good test coverage, it is recommended to only load
# this extension when testing.
#
# Related module: Sequel::LitRequireFrozen

#
module Sequel
  class LiteralString
    # The string used when creating the literal string (first argument to
    # Sequel::LiteralString.new). This may be nil if no string was provided,
    # or if the litral string was created before this extension was required.
    attr_reader :source

    def initialize(*a)
      @source = a.first
      super
    end
    # :nocov:
    ruby2_keywords :initialize if respond_to?(:ruby2_keywords, true)
    # :nocov:
  end

  module LitRequireFrozen
    # Error class raised for using unfrozen literal string.
    class Error < Sequel::Error
    end

    module DatabaseMethods
      def self.extended(db)
        db.extend_datasets(DatasetMethods)
      end

      # Check given SQL is frozen before running it.
      def run(sql, opts=OPTS)
        @default_dataset.with_sql(sql)
        super
      end
    end

    module DatasetMethods
      # Check given SQL is not an unfrozen string.
      def with_sql(sql, *args)
        _check_unfrozen_literal_string(sql)
        super
      end

      # Check that placeholder string is frozen (or all entries
      # in placeholder array are frozen).
      def placeholder_literal_string_sql_append(sql, pls)
        case str = pls.str
        when String
          _check_unfrozen_literal_string(str)
        when Array
          str.each do |s|
            _check_unfrozen_literal_string(s)
          end
        end

        super
      end

      private

      # Base method that other methods used to check for whether a string should be allowed
      # as literal SQL. Allows non-strings as well as frozen strings.
      def _check_unfrozen_literal_string(str)
        return if !str.is_a?(String) || str.frozen?

        if str.is_a?(LiteralString)
          _check_unfrozen_literal_string(str.source)
        else
          raise Error, "cannot treat unfrozen string as literal SQL: #{str.inspect}"
        end
      end

      # Check literal strings appended to SQL.
      def literal_literal_string_append(sql, v)
        _check_unfrozen_literal_string(v)
        super
      end

      # Check static SQL is not frozen.
      def static_sql(sql)
        _check_unfrozen_literal_string(sql)
        super
      end
    end
  end

  Dataset.register_extension(:lit_require_frozen, LitRequireFrozen::DatasetMethods)
  Database.register_extension(:lit_require_frozen, LitRequireFrozen::DatabaseMethods)
end
