# This extension allows Sequel's postgres adapter to automatically
# parameterize all common queries.  Sequel's default behavior has always
# been to literalize all arguments unless specifically using
# parameters (via :$arg placeholders and the prepare/call methods).
# This extension makes Sequel take all string, numeric, date, and
# time types and automatically turn them into parameters. Example:
#
#   # Default
#   DB[:test].where(:a=>1)
#   # SQL: SELECT * FROM test WHERE a = 1
#
#   DB.extend Sequel::Postgres::AutoParameterize::DatabaseMethods
#   DB[:test].where(:a=>1)
#   # SQL: SELECT * FROM test WHERE a = $1 (args: [1])
#
# This extension is not necessarily faster or more safe than the
# default behavior.  In some cases it is faster, such as when using
# large strings.  However, there are also some known issues with
# this approach:
#
# 1. Because of the way it operates, it has no context to make a
#    determination about whether to literalize an object or not.
#    For example, if it comes across an integer, it will turn it
#    into a parameter.  That breaks code such as:
#
#      DB[:table].select(:a, :b).order(2, 1)
#
#    Since it will use the following SQL (which isn't valid):
#
#      SELECT a, b FROM table ORDER BY $1, $2
#     
#    To work around this, you can either specify the columns
#    manually or use a literal string:
#
#      DB[:table].select(:a, :b).order(:b, :a)
#      DB[:table].select(:a, :b).order('2, 1'.lit)
#
# 2. In order to avoid many type errors, it attempts to guess the
#    appropriate type and automatically casts all placeholders.
#    Unfortunately, if the type guess is incorrect, the query will
#    be rejected.  For example, the following works without
#    automatic parameterization, but fails with it:
#
#      DB[:table].insert(:interval=>'1 day')
#
#    To work around this, you can just add the necessary casts
#    manually:
#
#      DB[:table].insert(:interval=>'1 day'.cast(:interval))
#
# You can also work around any issues that come up by disabling automatic
# parameterization by calling the no_auto_parameterize method on the
# dataset (which returns a clone of the dataset).
#
# It is likely there are other corner cases I am not yet aware of
# when using this extension, so use this extension with caution.
#
# This extension is only compatible when using the pg driver, not
# when using the old postgres driver or the postgres-pr driver.

module Sequel
  module Postgres
    # Enable automatically parameterizing queries by hijacking the
    # SQL query string that Sequel builds to also hold the array
    # of parameters.
    module AutoParameterize
      # String that holds an array of parameters
      class StringWithArray < ::String
        # The array of parameters used by this query.
        attr_reader :args

        # Add a new parameter to this query, which adds
        # the parameter to the array of parameters, and an
        # SQL placeholder to the query itself.
        def add_arg(s, type)
          @args ||= []
          @args << s
          self << "$#{@args.length}::#{type}"
        end

        # Show args when the string is inspected
        def inspect
          @args ? "#{self}; #{@args.inspect}".inspect : super
        end
      end

      module DatabaseMethods
        # Extend the database's datasets with the necessary code.
        def self.extended(db)
          db.extend_datasets(DatasetMethods)
        end

        # If the sql string has an embedded parameter array,
        # extract the arguments from that.
        def execute(sql, opts={})
          if sql.is_a?(StringWithArray) && (args = sql.args)
            opts = opts.merge(:arguments=>args)
          end
          super
        end
        
        # If the sql string has an embedded parameter array,
        # extract the arguments from that.
        def execute_insert(sql, opts={})
          if sql.is_a?(StringWithArray) && (args = sql.args)
            opts = opts.merge(:arguments=>args)
          end
          super
        end
      end

      module DatasetMethods
        # Return a clone of the dataset that will not do
        # automatic parameterization.
        def no_auto_parameterize
          clone(:no_auto_parameterize=>true)
        end

        # For strings, numeric arguments, and date/time arguments, add
        # them as parameters to the query instead of literalizing them
        # into the SQL.
        def literal_append(sql, v)
          if sql.is_a?(StringWithArray)
            case v
            when String
              case v
              when LiteralString
                super
              when Sequel::SQL::Blob
                sql.add_arg(v, :bytea)
              else
                sql.add_arg(v, :text)
              end
            when Bignum
              sql.add_arg(v, :int8)
            when Fixnum
              sql.add_arg(v, :int4)
            when Float
              sql.add_arg(v, :"double precision")
            when BigDecimal
              sql.add_arg(v, :numeric)
            when Sequel::SQLTime
              sql.add_arg(v, :time)
            when Time, DateTime
              sql.add_arg(v, :timestamp)
            when Date
              sql.add_arg(v, :date)
            else
              super
            end
          else
            super
          end
        end

        def use_cursor(*)
          super.no_auto_parameterize
        end

        protected

        # Disable automatic parameterization for prepared statements,
        # since they will use manual parameterization.
        def to_prepared_statement(*a)
          opts[:no_auto_parameterize] ? super : no_auto_parameterize.to_prepared_statement(*a)
        end

        private

        # Unless auto parameterization is turned off, use a string that
        # can store the parameterized arguments.
        def sql_string_origin
          opts[:no_auto_parameterize] ? super : StringWithArray.new
        end
      end
    end
  end
end
