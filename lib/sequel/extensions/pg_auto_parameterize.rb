# frozen-string-literal: true
#
# This extension changes Sequel's postgres adapter to automatically
# parameterize queries by default.  Sequel's default behavior has always
# been to literalize all arguments unless specifically using
# parameters (via :$arg placeholders and the Dataset#prepare/call methods).
# This extension makes Sequel use string, numeric, blob, date, and
# time types as parameters. Example:
#
#   # Default
#   DB[:test].where(:a=>1)
#   # SQL: SELECT * FROM test WHERE a = 1
#
#   DB.extension :pg_auto_parameterize
#   DB[:test].where(:a=>1)
#   # SQL: SELECT * FROM test WHERE a = $1 (args: [1])
#
# Other pg_* extensions that ship with Sequel and add support for
# PostgreSQL-specific types support automatically parameterizing those
# types when used with this extension.
#
# This extension is not generally faster than the default behavior.
# In some cases it is faster, such as when using large strings.
# However, the use of parameters avoids potential security issues,
# in case Sequel does not correctly literalize one of the arguments
# that this extension would automatically parameterize.
#
# There are some known issues with automatic parameterization:
#
# 1. In order to avoid most type errors, the extension attempts to guess
#    the appropriate type and automatically casts most placeholders,
#    except plain Ruby strings (which PostgreSQL treats as an unknown
#    type).
#
#    Unfortunately, if the type guess is incorrect, or a plain Ruby
#    string is used and PostgreSQL cannot determine the data type for it,
#    the query may result in a DatabaseError. To fix both issues, you can
#    explicitly cast values using <tt>Sequel.cast(value, type)</tt>, and
#    Sequel will cast to that type.
#
# 2. PostgreSQL supports a maximum of 65535 parameters per query.
#    Attempts to use a query with more than this number of parameters
#    will result in a Sequel::DatabaseError being raised. Sequel tries
#    to mitigate this issue by turning <tt>column IN (int, ...)</tt>
#    queries into <tt>column = ANY(CAST($ AS int8[]))</tt> using an
#    array parameter, to reduce the number of parameters. It also limits
#    inserting multiple rows at once to a maximum of 40 rows per query by
#    default.  While these mitigations handle the most common cases
#    where a large number of parameters would be used, there are other
#    cases.
#
# 3. Automatic parameterization will consider the same objects as
#    equivalent when building SQL.  However, for performance, it does
#    not perform equality checks.  So code such as:
#
#      DB[:t].select{foo('a').as(:f)}.group{foo('a')}
#      # SELECT foo('a') AS "f" FROM "t" GROUP BY foo('a')
#
#    Will get auto paramterized as:
#
#      # SELECT foo($1) AS "f" FROM "t" GROUP BY foo($2)
#
#    Which will result in a DatabaseError, since that is not valid SQL.
#
#    If you use the same expression, it will use the same parameter:
#
#      foo = Sequel.function(:foo, 'a')
#      DB[:t].select(foo.as(:f)).group(foo)
#      # SELECT foo($1) AS "f" FROM "t" GROUP BY foo($1)
#
#    Note that Dataset#select_group and similar methods that take arguments
#    used in multiple places in the SQL will generally handle this
#    automatically, since they will use the same objects:
#
#      DB[:t].select_group{foo('a').as(:f)}
#      # SELECT foo($1) AS "f" FROM "t" GROUP BY foo($1)
#
# You can work around any issues that come up by disabling automatic
# parameterization by calling the +no_auto_parameterize+ method on the
# dataset (which returns a clone of the dataset). You can avoid
# parameterization for specific values in the query by wrapping them
# with +Sequel.skip_pg_auto_param+.
#
# It is likely there are corner cases not mentioned above
# when using this extension.  Users are encouraged to provide feedback
# when using this extension if they come across such corner cases.
#
# This extension is only compatible when using the pg driver, not
# when using the sequel-postgres-pr, jeremyevans-postgres-pr, or
# postgres-pr drivers, as those do not support bound variables.
#
# Related module: Sequel::Postgres::AutoParameterize

module Sequel
  module Postgres
    # Enable automatically parameterizing queries.
    module AutoParameterize
      # SQL query string that also holds an array of parameters
      class QueryString < ::String
        # The array of parameters used by this query.
        attr_reader :args

        # Add a new parameter to this query, which adds
        # the parameter to the array of parameters, and an
        # SQL placeholder to the query itself.
        def add_arg(s)
          unless defined?(@args)
            @args = []
            @arg_map = {}
            @arg_map.compare_by_identity
          end

          unless pos = @arg_map[s]
            @args << s
            pos = @arg_map[s] = @args.length.to_s
          end
          self << '$' << pos
        end

        # Return a new QueryString with the given string appended
        # to the receiver, and the same arguments.
        def +(other)
          v = self.class.new(super)
          v.instance_variable_set(:@args, @args) if @args
          v
        end

        # Whether this query string currently supports
        # automatic parameterization.  Automatic parameterization
        # is disabled at certain points during query building where
        # PostgreSQL does not support it.
        def auto_param?
          !@skip_auto_param
        end

        # Skip automatic parameterization inside the passed block.
        # This is used during query generation to disable
        # automatic parameterization for clauses not supporting it.
        def skip_auto_param
          skip_auto_param = @skip_auto_param
          begin
            @skip_auto_param = true
            yield
          ensure
            @skip_auto_param = skip_auto_param
          end
        end

        # Freeze the stored arguments when freezing the query string.
        def freeze
          if @args
            @args.freeze
            @arg_map.freeze
          end
          super
        end

        # Show args when the query string is inspected
        def inspect
          @args ? "#{self}; #{@args.inspect}".inspect : super
        end

        def initialize_copy(other)
          super
          if args = other.instance_variable_get(:@args)
            @args = args.dup
            @arg_map = other.instance_variable_get(:@arg_map).dup
          end
        end
      end

      # Wrapper class that skips auto parameterization for the wrapped object.
      class SkipAutoParam < SQL::Wrapper
        def to_s_append(ds, sql)
          if sql.is_a?(QueryString)
            sql.skip_auto_param{super}
          else
            super
          end
        end
      end

      # PlacholderLiteralizer subclass with support for stored auto parameters.
      class PlaceholderLiteralizer < ::Sequel::Dataset::PlaceholderLiteralizer
        def initialize(dataset, fragments, final_sql, arity)
          s = dataset.sql.dup
          s.clear
          @sql_origin = s.freeze
          super
        end

        private

        def sql_origin
          @sql_origin.dup
        end
      end

      module DatabaseMethods
        def self.extended(db)
          unless (db.adapter_scheme == :postgres && USES_PG) || (db.adapter_scheme == :mock && db.database_type == :postgres)
            raise Error, "pg_auto_parameterize is only supported when using the postgres adapter with the pg driver"
          end
          db.extend_datasets(DatasetMethods)
        end

        # If the sql string has an embedded parameter array,
        # extract the parameter values from that.
        def execute(sql, opts={})
          if sql.is_a?(QueryString) && (args = sql.args)
            opts = opts.merge(:arguments=>args)
          end
          super
        end

        private

        # Disable auto_parameterization during COPY TABLE.
        def copy_table_sql(table, opts=OPTS)
          table = _no_auto_parameterize(table)
          super
        end

        # Disable auto_parameterization during CREATE TABLE AS.
        def create_table_as(name, sql, options)
          sql = _no_auto_parameterize(sql)
          super
        end

        # Disable auto_parameterization during CREATE VIEW.
        def create_view_sql(name, source, options)
          source = _no_auto_parameterize(source)
          super
        end

        # Disable automatic parameterization for the given table if supported.
        def _no_auto_parameterize(table)
          if table.is_a?(DatasetMethods)
            table.no_auto_parameterize
          else
            table
          end
        end
      end

      module DatasetMethods
        # Return a clone of the dataset that will not do
        # automatic parameterization.
        def no_auto_parameterize
          cached_dataset(:_no_auto_parameterize_ds) do
            @opts[:no_auto_parameterize] ? self : clone(:no_auto_parameterize=>true)
          end
        end

        # Do not add implicit typecasts for directly typecasted values,
        # since the user is presumably doing so to set the type, not convert
        # from the implicitly typecasted type.
        def cast_sql_append(sql, expr, type)
          if auto_param?(sql) && auto_param_type(expr)
            sql << 'CAST('
            sql.add_arg(expr)
            sql << ' AS ' << db.cast_type_literal(type).to_s << ')'
          else
            super
          end
        end

        # Transform column IN (int, ...) expressions into column = ANY($)
        # and column NOT IN (int, ...) expressions into column != ALL($)
        # using an integer array bound variable for the ANY/ALL argument.
        # This is the same optimization PostgreSQL performs internally,
        # but this reduces the number of bound variables.
        def complex_expression_sql_append(sql, op, args)
          case op
          when :IN, :"NOT IN"
            l, r = args
            if auto_param?(sql) && !l.is_a?(Array) && _integer_array?(r) && r.size > 1
              if op == :IN 
                op = :"="
                func = :ANY
              else
                op = :!=
                func = :ALL
              end
              args = [l, Sequel.function(func, Sequel.cast(_integer_array_auto_param(r), 'int8[]'))]
            end
          end

          super
        end

        # Parameterize insertion of multiple values
        def multi_insert_sql(columns, values)
          if @opts[:no_auto_parameterize]
            super
          else
            [clone(:multi_insert_values=>values.map{|r| Array(r)}).insert_sql(columns, LiteralString.new('VALUES '))]
          end
        end

        # For strings, numeric arguments, and date/time arguments, add
        # them as parameters to the query instead of literalizing them
        # into the SQL.
        def literal_append(sql, v)
          if auto_param?(sql) && (type = auto_param_type(v))
            sql.add_arg(v) << type
          else
            super
          end
        end

        # The class to use for placeholder literalizers.
        def placeholder_literalizer_class
          if @opts[:no_auto_parameterize]
            super
          else
            PlaceholderLiteralizer
          end
        end

        # Disable automatic parameterization when using a cursor.
        def use_cursor(*)
          super.no_auto_parameterize
        end

        # Store receiving dataset and args when with_sql is used with a method name symbol, so sql
        # can be parameterized correctly if used as a subselect.
        def with_sql(*a)
          ds = super 
          if Symbol === a[0]
            ds = ds.clone(:with_sql_dataset=>self, :with_sql_args=>a.freeze)
          end
          ds
        end

        protected

        # Disable automatic parameterization for prepared statements,
        # since they will use manual parameterization.
        def to_prepared_statement(*a)
          @opts[:no_auto_parameterize] ? super : no_auto_parameterize.to_prepared_statement(*a)
        end

        private

        # If auto parameterization is supported for the value, return a string
        # for the implicit typecast to use.  Return false/nil if the value should not be
        # automatically parameterized.
        def auto_param_type(v)
          case v
          when String
            case v
            when LiteralString
              false
            when Sequel::SQL::Blob
              "::bytea"
            else
              ""
            end
          when Integer
            ((v > 2147483647 || v < -2147483648) ? "::int8" : "::int4")
          when Float
            # PostgreSQL treats literal floats as numeric, not double precision
            # But older versions of PostgreSQL don't handle Infinity/NaN in numeric
            v.finite? ? "::numeric" : "::double precision"
          when BigDecimal
            "::numeric"
          when Sequel::SQLTime
            "::time"
          when Time
            "::#{@db.cast_type_literal(Time)}"
          when DateTime
            "::#{@db.cast_type_literal(DateTime)}"
          when Date
            "::date"
          else
            v.respond_to?(:sequel_auto_param_type) ? v.sequel_auto_param_type(self) : auto_param_type_fallback(v)
          end
        end

        # Allow other extensions to support auto parameterization in ways that do not
        # require adding the sequel_auto_param_type method.
        def auto_param_type_fallback(v)
          super if defined?(super)
        end

        # Whether the given query string currently supports automatic parameterization.
        def auto_param?(sql)
          sql.is_a?(QueryString) && sql.auto_param?
        end

        # Default the import slice to 40, since PostgreSQL supports a maximum of 1600
        # columns per table, and it supports a maximum of 65k parameters. Technically,
        # there can be more than one parameter per column, so this doesn't prevent going
        # over the limit, though it does make it less likely.
        def default_import_slice
          @opts[:no_auto_parameterize] ? super : 40
        end

        # Handle parameterization of multi_insert_sql
        def _insert_values_sql(sql, values)
          super

          if values = @opts[:multi_insert_values]
            expression_list_append(sql, values.map{|r| Array(r)})
          end
        end

        # Whether the given argument is an array of integers or NULL values, recursively.
        def _integer_array?(v)
          Array === v && v.all?{|x| nil == x || Integer === x}
        end

        # Create the bound variable string that will be used for the IN (int, ...) to = ANY($)
        # optimization for integer arrays.
        def _integer_array_auto_param(v)
          buf = String.new
          buf << '{'
          comma = false
          v.each do |x|
            if comma
              buf << ","
            else
              comma = true
            end

            buf << (x ? x.to_s : 'NULL')
          end
          buf << '}'
        end

        # Skip auto parameterization in LIMIT and OFFSET clauses
        def select_limit_sql(sql)
          if auto_param?(sql) && (@opts[:limit] || @opts[:offset])
            sql.skip_auto_param{super}
          else
            super
          end
        end

        # Skip auto parameterization in ORDER clause if used with
        # integer values indicating ordering by the nth column.
        def select_order_sql(sql)
          if auto_param?(sql) && (order = @opts[:order]) && order.any?{|o| Integer === o || (SQL::OrderedExpression === o && Integer === o.expression)}
            sql.skip_auto_param{super}
          else
            super
          end
        end

        # Skip auto parameterization in CTE CYCLE clause
        def select_with_sql_cte_search_cycle(sql,cte)
          if auto_param?(sql) && cte[:cycle]
            sql.skip_auto_param{super}
          else
            super
          end
        end

        # Unless auto parameterization is disabled, use a string that
        # can store the parameterized arguments.
        def sql_string_origin
          @opts[:no_auto_parameterize] ? super : QueryString.new
        end

        # A mutable string used as the prefix when explaining a query.
        def explain_sql_string_origin(opts)
          @opts[:no_auto_parameterize] ? super : (QueryString.new << super)
        end

        # If subquery uses with_sql with a method name symbol, get the dataset
        # with_sql was called on, and use that as the subquery, recording the
        # arguments to with_sql that will be used to calculate the sql.
        def subselect_sql_dataset(sql, ds)
          if ws_ds = ds.opts[:with_sql_dataset]
            super(sql, ws_ds).clone(:subselect_sql_args=>ds.opts[:with_sql_args])
          else
            super
          end
        end

        # If subquery used with_sql with a method name symbol, use the arguments to
        # with_sql to determine the sql, so that the subselect can be parameterized.
        def subselect_sql_append_sql(sql, ds)
          if args = ds.opts[:subselect_sql_args]
            ds.send(*args)
          else
            super
          end
        end

        # Use auto parameterization for datasets with static SQL using placeholders.
        def static_sql(sql)
          if @opts[:append_sql] || @opts[:no_auto_parameterize] || String === sql
            super
          else
            query_string = QueryString.new
            literal_append(query_string, sql)
            query_string
          end
        end
      end
    end
  end

  module SQL::Builders
    # Skip auto parameterization for the given object when building queries.
    def skip_pg_auto_param(v)
      Postgres::AutoParameterize::SkipAutoParam.new(v)
    end
  end

  Database.register_extension(:pg_auto_parameterize, Postgres::AutoParameterize::DatabaseMethods)
end
