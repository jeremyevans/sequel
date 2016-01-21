# frozen-string-literal: true

module Sequel
  module SqlAnywhere

    @convert_smallint_to_bool = true

    class << self
      # Whether to convert smallint values to bool, false by default.
      # Can also be overridden per dataset.
      attr_accessor :convert_smallint_to_bool
    end

    module DatabaseMethods
      extend Sequel::Database::ResetIdentifierMangling

      attr_reader :conversion_procs

      # Override the default SqlAnywhere.convert_smallint_to_bool setting for this database.
      attr_writer :convert_smallint_to_bool

      AUTO_INCREMENT = 'IDENTITY'.freeze
      SQL_BEGIN = "BEGIN TRANSACTION".freeze
      SQL_COMMIT = "COMMIT TRANSACTION".freeze
      SQL_ROLLBACK = "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION".freeze
      TEMPORARY = "GLOBAL TEMPORARY ".freeze
      SMALLINT_RE = /smallint/i.freeze
      DECIMAL_TYPE_RE = /numeric/io

      # Whether to convert smallint to boolean arguments for this dataset.
      # Defaults to the SqlAnywhere module setting.
      def convert_smallint_to_bool
        defined?(@convert_smallint_to_bool) ? @convert_smallint_to_bool : (@convert_smallint_to_bool = ::Sequel::SqlAnywhere.convert_smallint_to_bool)
      end

      # Sysbase Server uses the :sqlanywhere type.
      def database_type
        :sqlanywhere
      end

      def to_application_timestamp_sa(v)
        to_application_timestamp(v.to_s) if v
      end

      # Convert smallint type to boolean if convert_smallint_to_bool is true
      def schema_column_type(db_type)
        if convert_smallint_to_bool && db_type =~ SMALLINT_RE
          :boolean
        else
          super
        end
      end

      def schema_parse_table(table, opts)
        m = output_identifier_meth(opts[:dataset])
        im = input_identifier_meth(opts[:dataset])
        metadata_dataset.
         from{sa_describe_query("select * from #{im.call(table)}").as(:a)}.
         join(:syscolumn___b, :table_id=>:base_table_id, :column_id=>:base_column_id).
         order(:a__column_number).
         map do |row|
          auto_increment = row.delete(:is_autoincrement)
          row[:auto_increment] = auto_increment == 1 || auto_increment == true
          row[:primary_key] = row.delete(:pkey) == 'Y'
          row[:allow_null] = row[:nulls_allowed].is_a?(Fixnum) ? row.delete(:nulls_allowed) == 1 : row.delete(:nulls_allowed)
          row[:db_type] = row.delete(:domain_name)
          row[:type] = if row[:db_type] =~ DECIMAL_TYPE_RE and (row[:scale].is_a?(Fixnum) ? row[:scale] == 0 : !row[:scale])
            :integer
          else
            schema_column_type(row[:db_type])
          end
          row[:max_length] = row[:width] if row[:type] == :string
          [m.call(row.delete(:name)), row]
        end
      end

      def indexes(table, opts = OPTS)
        m = output_identifier_meth
        im = input_identifier_meth
        indexes = {}
        metadata_dataset.
         from(:dbo__sysobjects___z).
         select(:z__name___table_name, :i__name___index_name, :si__indextype___type, :si__colnames___columns).
         join(:dbo__sysindexes___i, :id___i=> :id___z).
         join(:sys__sysindexes___si, :iname=> :name___i).
         where(:z__type => 'U', :table_name=>im.call(table)).
         each do |r|
          indexes[m.call(r[:index_name])] =
            {:unique=>(r[:type].downcase=='unique'),
             :columns=>r[:columns].split(',').map{|v| m.call(v.split(' ').first)}} unless r[:type].downcase == 'primary key'
        end
        indexes
      end

      def foreign_key_list(table, opts=OPTS)
        m = output_identifier_meth
        im = input_identifier_meth
        fk_indexes = {}
        metadata_dataset.
         from(:sys__sysforeignkey___fk).
         select(:fk__role___name, :fks__columns___column_map, :si__indextype___type, :si__colnames___columns, :fks__primary_tname___table_name).
         join(:sys__sysforeignkeys___fks, :role => :role).
         join_table(:inner, :sys__sysindexes___si, [:iname=> :fk__role], {:implicit_qualifier => :fk}).
         where(:fks__foreign_tname=>im.call(table)).
         each do |r|
          unless r[:type].downcase == 'primary key'
            fk_indexes[r[:name]] =
              {:name=>m.call(r[:name]),
               :columns=>r[:columns].split(',').map{|v| m.call(v.split(' ').first)},
               :table=>m.call(r[:table_name]),
               :key=>r[:column_map].split(',').map{|v| m.call(v.split(' IS ').last)}}
          end
        end
        fk_indexes.values
      end

      def tables(opts=OPTS)
        tables_and_views('U', opts)
      end

      def views(opts=OPTS)
        tables_and_views('V', opts)
      end

      private

      DATABASE_ERROR_REGEXPS = {
        /would not be unique|Primary key for table.+is not unique/ => Sequel::UniqueConstraintViolation,
        /Column .* in table .* cannot be NULL/ => Sequel::NotNullConstraintViolation,
        /Constraint .* violated: Invalid value in table .*/ => Sequel::CheckConstraintViolation,
        /No primary key value for foreign key .* in table .*/ => Sequel::ForeignKeyConstraintViolation,
        /Primary key for row in table .* is referenced by foreign key .* in table .*/ => Sequel::ForeignKeyConstraintViolation
      }.freeze

      def database_error_regexps
        DATABASE_ERROR_REGEXPS
      end

      # Sybase uses the IDENTITY column for autoincrementing columns.
      def auto_increment_sql
        AUTO_INCREMENT
      end
      
      # SQL fragment for marking a table as temporary
      def temporary_table_sql
        TEMPORARY
      end

      # SQL to BEGIN a transaction.
      def begin_transaction_sql
        SQL_BEGIN
      end

      # SQL to ROLLBACK a transaction.
      def rollback_transaction_sql
        SQL_ROLLBACK
      end

      # SQL to COMMIT a transaction.
      def commit_transaction_sql
        SQL_COMMIT
      end

      # Sybase has both datetime and timestamp classes, most people are going
      # to want datetime
      def type_literal_generic_datetime(column)
        :datetime
      end

      # Sybase has both datetime and timestamp classes, most people are going
      # to want datetime
      def type_literal_generic_time(column)
        column[:only_time] ? :time : :datetime
      end
      
      # Sybase doesn't have a true boolean class, so it uses integer
      def type_literal_generic_trueclass(column)
        :smallint
      end

      # SQLAnywhere uses image type for blobs
      def type_literal_generic_file(column)
        :image
      end

      # Sybase specific syntax for altering tables.
      def alter_table_sql(table, op)
        case op[:op]
        when :add_column
          "ALTER TABLE #{quote_schema_table(table)} ADD #{column_definition_sql(op)}"
        when :drop_column
          "ALTER TABLE #{quote_schema_table(table)} DROP #{column_definition_sql(op)}"
        when :drop_constraint
          case op[:type]
          when :primary_key
            "ALTER TABLE #{quote_schema_table(table)} DROP PRIMARY KEY"
          when :foreign_key
            if op[:name] || op[:columns]
              name = op[:name] || foreign_key_name(table, op[:columns])
              if name
                "ALTER TABLE #{quote_schema_table(table)} DROP FOREIGN KEY #{quote_identifier(name)}"
              end
            end
          else
            super
          end
        when :rename_column
          "ALTER TABLE #{quote_schema_table(table)} RENAME #{quote_identifier(op[:name])} TO #{quote_identifier(op[:new_name].to_s)}"
        when :set_column_type
          "ALTER TABLE #{quote_schema_table(table)} ALTER #{quote_identifier(op[:name])} #{type_literal(op)}"
        when :set_column_null
          "ALTER TABLE #{quote_schema_table(table)} ALTER #{quote_identifier(op[:name])} #{'NOT ' unless op[:null]}NULL"
        when :set_column_default
          "ALTER TABLE #{quote_schema_table(table)} ALTER #{quote_identifier(op[:name])} DEFAULT #{literal(op[:default])}"
        else
          super(table, op)
        end
      end

      # SqlAnywhere doesn't support CREATE TABLE AS, it only supports SELECT INTO.
      # Emulating CREATE TABLE AS using SELECT INTO is only possible if a dataset
      # is given as the argument, it can't work with a string, so raise an
      # Error if a string is given.
      def create_table_as(name, ds, options)
        raise(Error, "must provide dataset instance as value of create_table :as option on SqlAnywhere") unless ds.is_a?(Sequel::Dataset)
        run(ds.into(name).sql)
      end

      # Use SP_RENAME to rename the table
      def rename_table_sql(name, new_name)
        "ALTER TABLE #{quote_schema_table(name)} RENAME #{quote_schema_table(new_name)}"
      end

      def tables_and_views(type, opts=OPTS)
        m = output_identifier_meth
        metadata_dataset.
          from(:sysobjects___a).
          where(:a__type=>type).
          select_map(:a__name).
          map{|n| m.call(n)}
      end
      
      # SQLAnywhere supports views with check option, but not local.
      def view_with_check_option_support
        true
      end
    end

    module DatasetMethods
      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      WILDCARD = LiteralString.new('%').freeze
      TOP = " TOP ".freeze
      START_AT = " START AT ".freeze
      SQL_WITH_RECURSIVE = "WITH RECURSIVE ".freeze
      DATE_FUNCTION = 'today()'.freeze
      NOW_FUNCTION = 'now()'.freeze
      DATEPART = 'datepart'.freeze
      REGEXP = 'REGEXP'.freeze
      NOT_REGEXP = 'NOT REGEXP'.freeze
      APOS = Dataset::APOS
      APOS_RE = Dataset::APOS_RE
      DOUBLE_APOS = Dataset::DOUBLE_APOS
      BACKSLASH_RE = /\\/.freeze
      QUAD_BACKSLASH = "\\\\\\\\".freeze
      BLOB_START = "0x".freeze
      HSTAR = "H*".freeze
      CROSS_APPLY = 'CROSS APPLY'.freeze
      OUTER_APPLY = 'OUTER APPLY'.freeze
      ONLY_OFFSET = " TOP 2147483647".freeze

      Dataset.def_sql_method(self, :insert, %w'with insert into columns values')
      Dataset.def_sql_method(self, :select, %w'with select distinct limit columns into from join where group having order compounds lock')

      # Whether to convert smallint to boolean arguments for this dataset.
      # Defaults to the SqlAnywhere module setting.
      def convert_smallint_to_bool
        defined?(@convert_smallint_to_bool) ? @convert_smallint_to_bool : (@convert_smallint_to_bool = @db.convert_smallint_to_bool)
      end

      # Override the default SqlAnywhere.convert_smallint_to_bool setting for this dataset.
      attr_writer :convert_smallint_to_bool

      def supports_cte?(type=:select)
        type == :select || type == :insert
      end

      # SQLAnywhere supports GROUPING SETS
      def supports_grouping_sets?
        true
      end

      def supports_multiple_column_in?
        false
      end

      def supports_where_true?
        false
      end

      def supports_is_true?
        false
      end

      def supports_join_using?
        false
      end

      def supports_timestamp_usecs?
        false
      end

      # Uses CROSS APPLY to join the given table into the current dataset.
      def cross_apply(table)
        join_table(:cross_apply, table)
      end

      # SqlAnywhere requires recursive CTEs to have column aliases.
      def recursive_cte_requires_column_aliases?
        true
      end

      # SQLAnywhere uses + for string concatenation, and LIKE is case insensitive by default.
      def complex_expression_sql_append(sql, op, args)
        case op
        when :'||'
          super(sql, :+, args)
        when :<<, :>>
          complex_expression_emulate_append(sql, op, args)
        when :LIKE, :"NOT LIKE"
          sql << Sequel::Dataset::PAREN_OPEN
          literal_append(sql, args.at(0))
          sql << Sequel::Dataset::SPACE << (op == :LIKE ? REGEXP : NOT_REGEXP) << Sequel::Dataset::SPACE
          pattern = String.new
          last_c = ''
          args.at(1).each_char do |c|
            if  c == '_' and not pattern.end_with?('\\') and last_c != '\\'
              pattern << '.'
            elsif c == '%' and not pattern.end_with?('\\') and last_c != '\\'
              pattern << '.*'
            elsif c == '[' and not pattern.end_with?('\\') and last_c != '\\'
              pattern << '\['
            elsif c == ']' and not pattern.end_with?('\\') and last_c != '\\'
              pattern << '\]'
            elsif c == '*' and not pattern.end_with?('\\') and last_c != '\\'
              pattern << '\*'
            elsif c == '?' and not pattern.end_with?('\\') and last_c != '\\'
              pattern << '\?'
            else
              pattern << c
            end
            if c == '\\' and last_c == '\\'
              last_c = ''
            else
              last_c = c
            end
          end
          literal_append(sql, pattern)
          sql << Sequel::Dataset::ESCAPE
          literal_append(sql, Sequel::Dataset::BACKSLASH)
          sql << Sequel::Dataset::PAREN_CLOSE
        when :ILIKE, :"NOT ILIKE"
          super(sql, (op == :ILIKE ? :LIKE : :"NOT LIKE"), args)
        when :extract
          sql << DATEPART + Sequel::Dataset::PAREN_OPEN
          literal_append(sql, args.at(0))
          sql << ','
          literal_append(sql, args.at(1))
          sql << Sequel::Dataset::PAREN_CLOSE
        else
          super
        end
      end

      # SqlAnywhere uses \\ to escape metacharacters, but a ']' should not be escaped
      def escape_like(string)
        string.gsub(/[\\%_\[]/){|m| "\\#{m}"}
      end

      # Use Date() and Now() for CURRENT_DATE and CURRENT_TIMESTAMP
      def constant_sql_append(sql, constant)
        case constant
        when :CURRENT_DATE
          sql << DATE_FUNCTION
        when :CURRENT_TIMESTAMP, :CURRENT_TIME
          sql << NOW_FUNCTION
        else
          super
        end
      end

      # Specify a table for a SELECT ... INTO query.
      def into(table)
        clone(:into => table)
      end

      private

      # Use 1 for true on Sybase
      def literal_true
        BOOL_TRUE
      end

      # Use 0 for false on Sybase
      def literal_false
        BOOL_FALSE
      end

      # SQL fragment for String.  Doubles \ and ' by default.
      def literal_string_append(sql, v)
        sql << APOS << v.gsub(BACKSLASH_RE, QUAD_BACKSLASH).gsub(APOS_RE, DOUBLE_APOS) << APOS
      end

      # SqlAnywhere uses a preceding X for hex escaping strings
      def literal_blob_append(sql, v)
        if v.empty?
          literal_append(sql, "")
        else
          sql << BLOB_START << v.unpack(HSTAR).first
        end
      end

      # Sybase supports multiple rows in INSERT.
      def multi_insert_sql_strategy
        :values
      end

      def select_into_sql(sql)
        if i = @opts[:into]
          sql << Sequel::Dataset::INTO
          identifier_append(sql, i)
        end
      end

      # Sybase uses TOP N for limit.  For Sybase TOP (N) is used
      # to allow the limit to be a bound variable.
      def select_limit_sql(sql)
        l = @opts[:limit]
        o = @opts[:offset]
        if l || o
          if l
            sql << TOP
            literal_append(sql, l)
          else
            sql << ONLY_OFFSET
          end

          if o 
            sql << START_AT + "("
            literal_append(sql, o)
            sql << " + 1)"
          end
        end
      end

      # Use WITH RECURSIVE instead of WITH if any of the CTEs is recursive
      def select_with_sql_base
        opts[:with].any?{|w| w[:recursive]} ? SQL_WITH_RECURSIVE : super
      end

      def join_type_sql(join_type)
        case join_type
        when :cross_apply
          CROSS_APPLY
        when :outer_apply
          OUTER_APPLY
        else
          super
        end
      end

      # SQLAnywhere supports millisecond timestamp precision.
      def timestamp_precision
        3
      end
    end
  end
end
