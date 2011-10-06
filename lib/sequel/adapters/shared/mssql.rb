Sequel.require 'adapters/utils/emulate_offset_with_row_number'

module Sequel
  Dataset::NON_SQL_OPTIONS << :disable_insert_output
  module MSSQL
    module DatabaseMethods
      AUTO_INCREMENT = 'IDENTITY(1,1)'.freeze
      SERVER_VERSION_RE = /^(\d+)\.(\d+)\.(\d+)/.freeze
      SERVER_VERSION_SQL = "SELECT CAST(SERVERPROPERTY('ProductVersion') AS varchar)".freeze
      SQL_BEGIN = "BEGIN TRANSACTION".freeze
      SQL_COMMIT = "COMMIT TRANSACTION".freeze
      SQL_ROLLBACK = "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION".freeze
      SQL_ROLLBACK_TO_SAVEPOINT = 'IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION autopoint_%d'.freeze
      SQL_SAVEPOINT = 'SAVE TRANSACTION autopoint_%d'.freeze
      
      # Whether to use N'' to quote strings, which allows unicode characters inside the
      # strings.  True by default for compatibility, can be set to false for a possible
      # performance increase.  This sets the default for all datasets created from this
      # Database object.
      attr_accessor :mssql_unicode_strings

      # The types to check for 0 scale to transform :decimal types
      # to :integer.
      DECIMAL_TYPE_RE = /number|numeric|decimal/io

      # Microsoft SQL Server uses the :mssql type.
      def database_type
        :mssql
      end
      
      # The version of the MSSQL server, as an integer (e.g. 10001600 for
      # SQL Server 2008 Express).
      def server_version(server=nil)
        return @server_version if @server_version
        @server_version = synchronize(server) do |conn|
          (conn.server_version rescue nil) if conn.respond_to?(:server_version)
        end
        unless @server_version
          m = SERVER_VERSION_RE.match(fetch(SERVER_VERSION_SQL).single_value.to_s)
          @server_version = (m[1].to_i * 1000000) + (m[2].to_i * 10000) + m[3].to_i
        end
        @server_version
      end
        
      # MSSQL supports savepoints, though it doesn't support committing/releasing them savepoint
      def supports_savepoints?
        true
      end
      
      # MSSQL supports transaction isolation levels
      def supports_transaction_isolation_levels?
        true
      end

      # Microsoft SQL Server supports using the INFORMATION_SCHEMA to get
      # information on tables.
      def tables(opts={})
        information_schema_tables('BASE TABLE', opts)
      end

      # Microsoft SQL Server supports using the INFORMATION_SCHEMA to get
      # information on views.
      def views(opts={})
        information_schema_tables('VIEW', opts)
      end

      private
      
      # MSSQL uses the IDENTITY(1,1) column for autoincrementing columns.
      def auto_increment_sql
        AUTO_INCREMENT
      end
      
      # MSSQL specific syntax for altering tables.
      def alter_table_sql(table, op)
        case op[:op]
        when :add_column
          "ALTER TABLE #{quote_schema_table(table)} ADD #{column_definition_sql(op)}"
        when :rename_column
          "sp_rename #{literal("#{quote_schema_table(table)}.#{quote_identifier(op[:name])}")}, #{literal(op[:new_name].to_s)}, 'COLUMN'"
        when :set_column_type
          "ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{quote_identifier(op[:name])} #{type_literal(op)}"
        when :set_column_null
          sch = schema(table).find{|k,v| k.to_s == op[:name].to_s}.last
          type = sch[:db_type]
          if [:string, :decimal].include?(sch[:type]) and size = (sch[:max_chars] || sch[:column_size])
            type += "(#{size}#{", #{sch[:scale]}" if sch[:scale] && sch[:scale].to_i > 0})"
          end
          "ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{quote_identifier(op[:name])} #{type_literal(:type=>type)} #{'NOT ' unless op[:null]}NULL"
        when :set_column_default
          "ALTER TABLE #{quote_schema_table(table)} ADD CONSTRAINT #{quote_identifier("sequel_#{table}_#{op[:name]}_def")} DEFAULT #{literal(op[:default])} FOR #{quote_identifier(op[:name])}"
        else
          super(table, op)
        end
      end
      
      # SQL to start a new savepoint
      def begin_savepoint_sql(depth)
        SQL_SAVEPOINT % depth
      end

      # SQL to BEGIN a transaction.
      def begin_transaction_sql
        SQL_BEGIN
      end
      
      # Commit the active transaction on the connection, does not commit/release
      # savepoints.
      def commit_transaction(conn, opts={})
        log_connection_execute(conn, commit_transaction_sql) unless Thread.current[:sequel_transaction_depth] > 1
      end

      # SQL to COMMIT a transaction.
      def commit_transaction_sql
        SQL_COMMIT
      end
        
      # MSSQL uses the name of the table to decide the difference between
      # a regular and temporary table, with temporary table names starting with
      # a #.
      def create_table_sql(name, generator, options)
        "CREATE TABLE #{quote_schema_table(options[:temp] ? "##{name}" : name)} (#{column_list_sql(generator)})"
      end
      
      # The SQL to drop an index for the table.
      def drop_index_sql(table, op)
        "DROP INDEX #{quote_identifier(op[:name] || default_index_name(table, op[:columns]))} ON #{quote_schema_table(table)}"
      end
      
      # Backbone of the tables and views support.
      def information_schema_tables(type, opts)
        m = output_identifier_meth
        metadata_dataset.from(:information_schema__tables___t).
          select(:table_name).
          filter(:table_type=>type, :table_schema=>(opts[:schema]||default_schema||'dbo').to_s).
          map{|x| m.call(x[:table_name])}
      end

      # Always quote identifiers in the metadata_dataset, so schema parsing works.
      def metadata_dataset
        ds = super
        ds.quote_identifiers = true
        ds
      end
      
      # Use sp_rename to rename the table
      def rename_table_sql(name, new_name)
        "sp_rename #{literal(quote_schema_table(name))}, #{quote_identifier(schema_and_table(new_name).pop)}"
      end
      
      # SQL to rollback to a savepoint
      def rollback_savepoint_sql(depth)
        SQL_ROLLBACK_TO_SAVEPOINT % depth
      end
      
      # SQL to ROLLBACK a transaction.
      def rollback_transaction_sql
        SQL_ROLLBACK
      end
      
      # The closest MSSQL equivalent of a boolean datatype is the bit type.
      def schema_column_type(db_type)
        case db_type
        when /\A(bit)\z/io
          :boolean
        else
          super
        end
      end

      # MSSQL uses the INFORMATION_SCHEMA to hold column information.  This method does
      # not support the parsing of primary key information.
      def schema_parse_table(table_name, opts)
        m = output_identifier_meth
        m2 = input_identifier_meth
        ds = metadata_dataset.from(:information_schema__tables___t).
         join(:information_schema__columns___c, :table_catalog=>:table_catalog,
              :table_schema => :table_schema, :table_name => :table_name).
         select(:column_name___column, :data_type___db_type, :character_maximum_length___max_chars, :column_default___default, :is_nullable___allow_null, :numeric_precision___column_size, :numeric_scale___scale).
         filter(:c__table_name=>m2.call(table_name.to_s))
        if schema = opts[:schema] || default_schema
          ds.filter!(:c__table_schema=>schema)
        end
        ds.map do |row|
          row[:allow_null] = row[:allow_null] == 'YES' ? true : false
          row[:default] = nil if blank_object?(row[:default])
          row[:type] = if row[:db_type] =~ DECIMAL_TYPE_RE && row[:scale] == 0
            :integer
          else
            schema_column_type(row[:db_type])
          end
          [m.call(row.delete(:column)), row]
        end
      end

      # Set the mssql_unicode_strings settings from the given options.
      def set_mssql_unicode_strings
        @mssql_unicode_strings = typecast_value_boolean(@opts.fetch(:mssql_unicode_strings, true))
      end
      
      # MSSQL has both datetime and timestamp classes, most people are going
      # to want datetime
      def type_literal_generic_datetime(column)
        :datetime
      end

      # MSSQL has both datetime and timestamp classes, most people are going
      # to want datetime
      def type_literal_generic_time(column)
        column[:only_time] ? :time : :datetime
      end
      
      # MSSQL doesn't have a true boolean class, so it uses bit
      def type_literal_generic_trueclass(column)
        :bit
      end
      
      # MSSQL uses varbinary(max) type for blobs
      def type_literal_generic_file(column)
        :'varbinary(max)'
      end

      # support for clustered index type
      def index_definition_sql(table_name, index)
        index_name = index[:name] || default_index_name(table_name, index[:columns])
        clustered = index[:type] == :clustered
        if index[:where]
          raise Error, "Partial indexes are not supported for this database"
        else
          "CREATE #{'UNIQUE ' if index[:unique]}#{'CLUSTERED ' if clustered}INDEX #{quote_identifier(index_name)} ON #{quote_schema_table(table_name)} #{literal(index[:columns])}"
        end
      end
    end
  
    module DatasetMethods
      include EmulateOffsetWithRowNumber

      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      COMMA_SEPARATOR = ', '.freeze
      DELETE_CLAUSE_METHODS = Dataset.clause_methods(:delete, %w'with from output from2 where')
      INSERT_CLAUSE_METHODS = Dataset.clause_methods(:insert, %w'with into columns output values')
      SELECT_CLAUSE_METHODS = Dataset.clause_methods(:select, %w'with distinct limit columns into from lock join where group having order compounds')
      UPDATE_CLAUSE_METHODS = Dataset.clause_methods(:update, %w'with table set output from where')
      NOLOCK = ' WITH (NOLOCK)'.freeze
      UPDLOCK = ' WITH (UPDLOCK)'.freeze
      WILDCARD = LiteralString.new('*').freeze
      CONSTANT_MAP = {:CURRENT_DATE=>'CAST(CURRENT_TIMESTAMP AS DATE)'.freeze, :CURRENT_TIME=>'CAST(CURRENT_TIMESTAMP AS TIME)'.freeze}
      EXTRACT_MAP = {:year=>"yy", :month=>"m", :day=>"d", :hour=>"hh", :minute=>"n", :second=>"s"}

      # Allow overriding of the mssql_unicode_strings option at the dataset level.
      attr_accessor :mssql_unicode_strings

      # Copy the mssql_unicode_strings option from the +db+ object.
      def initialize(db, opts={})
        super
        @mssql_unicode_strings = db.mssql_unicode_strings
      end

      # MSSQL uses + for string concatenation, and LIKE is case insensitive by default.
      def complex_expression_sql(op, args)
        case op
        when :'||'
          super(:+, args)
        when :ILIKE
          super(:LIKE, args)
        when :"NOT ILIKE"
          super(:"NOT LIKE", args)
        when :<<
          complex_expression_arg_pairs(args){|a, b| "(#{literal(a)} * POWER(2, #{literal(b)}))"}
        when :>>
          complex_expression_arg_pairs(args){|a, b| "(#{literal(a)} / POWER(2, #{literal(b)}))"}
        when :extract
          part = args.at(0)
          raise(Sequel::Error, "unsupported extract argument: #{part.inspect}") unless format = EXTRACT_MAP[part]
          expr = literal(args.at(1))
          s = "datepart(#{format}, #{expr})"
          s = "CAST((#{s} + datepart(ns, #{expr})/1000000000.0) AS double precision)" if part == :second
          s
        else
          super(op, args)
        end
      end
      
      # MSSQL doesn't support the SQL standard CURRENT_DATE or CURRENT_TIME
      def constant_sql(constant)
        CONSTANT_MAP[constant] || super
      end
      
      # Disable the use of INSERT OUTPUT
      def disable_insert_output
        clone(:disable_insert_output=>true)
      end

      # Disable the use of INSERT OUTPUT, modifying the receiver
      def disable_insert_output!
        mutation_method(:disable_insert_output)
      end

      # When returning all rows, if an offset is used, delete the row_number column
      # before yielding the row.
      def fetch_rows(sql, &block)
        @opts[:offset] ? super(sql){|r| r.delete(row_number_column); yield r} : super(sql, &block)
      end
      
      # MSSQL uses the CONTAINS keyword for full text search
      def full_text_search(cols, terms, opts = {})
        filter("CONTAINS (#{literal(cols)}, #{literal(terms)})")
      end

      # Use the OUTPUT clause to get the value of all columns for the newly inserted record.
      def insert_select(*values)
        return unless supports_insert_select?
        naked.clone(default_server_opts(:sql=>output(nil, [SQL::ColumnAll.new(:inserted)]).insert_sql(*values))).single_record
      end

      # Specify a table for a SELECT ... INTO query.
      def into(table)
        clone(:into => table)
      end

      # MSSQL uses a UNION ALL statement to insert multiple values at once.
      def multi_insert_sql(columns, values)
        [insert_sql(columns, LiteralString.new(values.map {|r| "SELECT #{expression_list(r)}" }.join(" UNION ALL ")))]
      end

      # Allows you to do a dirty read of uncommitted data using WITH (NOLOCK).
      def nolock
        lock_style(:dirty)
      end

      # Include an OUTPUT clause in the eventual INSERT, UPDATE, or DELETE query.
      #
      # The first argument is the table to output into, and the second argument
      # is either an Array of column values to select, or a Hash which maps output
      # column names to selected values, in the style of #insert or #update.
      #
      # Output into a returned result set is not currently supported.
      #
      # Examples:
      #
      #   dataset.output(:output_table, [:deleted__id, :deleted__name])
      #   dataset.output(:output_table, :id => :inserted__id, :name => :inserted__name)
      def output(into, values)
        raise(Error, "SQL Server versions 2000 and earlier do not support the OUTPUT clause") unless supports_output_clause?
        output = {}
        case values
          when Hash
            output[:column_list], output[:select_list] = values.keys, values.values
          when Array
            output[:select_list] = values
        end
        output[:into] = into
        clone({:output => output})
      end

      # An output method that modifies the receiver.
      def output!(into, values)
        mutation_method(:output, into, values)
      end

      # MSSQL uses [] to quote identifiers
      def quoted_identifier(name)
        "[#{name}]"
      end
      
      # The version of the database server.
      def server_version
        db.server_version(@opts[:server])
      end

      # MSSQL supports insert_select via the OUTPUT clause.
      def supports_insert_select?
        supports_output_clause? && !opts[:disable_insert_output]
      end

      # MSSQL 2005+ supports INTERSECT and EXCEPT
      def supports_intersect_except?
        is_2005_or_later?
      end
      
      # MSSQL does not support IS TRUE
      def supports_is_true?
        false
      end
      
      # MSSQL doesn't support JOIN USING
      def supports_join_using?
        false
      end

      # MSSQL 2005+ supports modifying joined datasets
      def supports_modifying_joins?
        is_2005_or_later?
      end

      # MSSQL does not support multiple columns for the IN/NOT IN operators
      def supports_multiple_column_in?
        false
      end
      
      # MSSQL 2005+ supports the output clause.
      def supports_output_clause?
        is_2005_or_later?
      end

      # MSSQL 2005+ supports window functions
      def supports_window_functions?
        true
      end

      # MSSQL cannot use WHERE 1.
      def supports_where_true?
        false
      end
      
      protected
      # MSSQL does not allow ordering in sub-clauses unless 'top' (limit) is specified
      def aggregate_dataset
        (options_overlap(Sequel::Dataset::COUNT_FROM_SELF_OPTS) && !options_overlap([:limit])) ? unordered.from_self : super
      end

      private

      def is_2005_or_later?
        server_version >= 9000000
      end

      # MSSQL supports the OUTPUT clause for DELETE statements.
      # It also allows prepending a WITH clause.
      def delete_clause_methods
        DELETE_CLAUSE_METHODS
      end

      # Only include the primary table in the main delete clause
      def delete_from_sql(sql)
        sql << " FROM #{source_list(@opts[:from][0..0])}"
      end

      # MSSQL supports FROM clauses in DELETE and UPDATE statements.
      def delete_from2_sql(sql)
        if joined_dataset?
          select_from_sql(sql)
          select_join_sql(sql)
        end
      end
      alias update_from_sql delete_from2_sql
      
      # Handle the with clause for delete, insert, and update statements
      # to be the same as the insert statement.
      def delete_with_sql(sql)
        select_with_sql(sql)
      end
      alias insert_with_sql delete_with_sql
      alias update_with_sql delete_with_sql
      
      # MSSQL raises an error if you try to provide more than 3 decimal places
      # for a fractional timestamp.  This probably doesn't work for smalldatetime
      # fields.
      def format_timestamp_usec(usec)
        sprintf(".%03d", usec/1000)
      end

      # MSSQL supports the OUTPUT clause for INSERT statements.
      # It also allows prepending a WITH clause.
      def insert_clause_methods
        INSERT_CLAUSE_METHODS
      end

      # Use OUTPUT INSERTED.* to return all columns of the inserted row,
      # for use with the prepared statement code.
      def insert_output_sql(sql)
        if @opts.has_key?(:returning)
          sql << " OUTPUT INSERTED.*"
        else
          output_sql(sql)
        end
      end

      # MSSQL uses a literal hexidecimal number for blob strings
      def literal_blob(v)
        blob = '0x'
        v.each_byte{|x| blob << sprintf('%02x', x)}
        blob
      end
      
      # Optionally use unicode string syntax for all strings. Don't double
      # backslashes.
      def literal_string(v)
        "#{'N' if mssql_unicode_strings}'#{v.gsub(/'/, "''")}'"
      end
      
      # Use 0 for false on MSSQL
      def literal_false
        BOOL_FALSE
      end

      # Use 1 for true on MSSQL
      def literal_true
        BOOL_TRUE
      end
      
      # MSSQL adds the limit before the columns
      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end

      def select_into_sql(sql)
        sql << " INTO #{table_ref(@opts[:into])}" if @opts[:into]
      end

      # MSSQL uses TOP N for limit.  For MSSQL 2005+ TOP (N) is used
      # to allow the limit to be a bound variable.
      def select_limit_sql(sql)
        if l = @opts[:limit]
          l = literal(l)
          l = "(#{l})" if server_version >= 9000000
          sql << " TOP #{l}"
        end
      end

      # Support different types of locking styles
      def select_lock_sql(sql)
        case @opts[:lock]
        when :update
          sql << UPDLOCK
        when :dirty
          sql << NOLOCK
        else
          super
        end
      end

      # SQL fragment for MSSQL's OUTPUT clause.
      def output_sql(sql)
        return unless supports_output_clause?
        return unless output = @opts[:output]
        sql << " OUTPUT #{column_list(output[:select_list])}"
        if into = output[:into]
          sql << " INTO #{table_ref(into)}"
          if column_list = output[:column_list]
            cl = []
            column_list.each { |k, v| cl << literal(String === k ? k.to_sym : k) }
            sql << " (#{cl.join(COMMA_SEPARATOR)})"
          end
        end
      end
      alias delete_output_sql output_sql
      alias update_output_sql output_sql

      # MSSQL supports the OUTPUT clause for UPDATE statements.
      # It also allows prepending a WITH clause.
      def update_clause_methods
        UPDATE_CLAUSE_METHODS
      end

      # Only include the primary table in the main update clause
      def update_table_sql(sql)
        sql << " #{source_list(@opts[:from][0..0])}"
      end
    end
  end
end
