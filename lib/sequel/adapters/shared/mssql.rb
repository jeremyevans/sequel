# frozen-string-literal: true

Sequel.require %w'emulate_offset_with_row_number split_alter_table', 'adapters/utils'

module Sequel
  Dataset::NON_SQL_OPTIONS << :disable_insert_output
  module MSSQL
    module DatabaseMethods
      extend Sequel::Database::ResetIdentifierMangling

      AUTO_INCREMENT = 'IDENTITY(1,1)'.freeze
      SERVER_VERSION_RE = /^(\d+)\.(\d+)\.(\d+)/.freeze
      SERVER_VERSION_SQL = "SELECT CAST(SERVERPROPERTY('ProductVersion') AS varchar)".freeze
      SQL_BEGIN = "BEGIN TRANSACTION".freeze
      SQL_COMMIT = "COMMIT TRANSACTION".freeze
      SQL_ROLLBACK = "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION".freeze
      SQL_ROLLBACK_TO_SAVEPOINT = 'IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION autopoint_%d'.freeze
      SQL_SAVEPOINT = 'SAVE TRANSACTION autopoint_%d'.freeze
      MSSQL_DEFAULT_RE = /\A(?:\(N?('.*')\)|\(\((-?\d+(?:\.\d+)?)\)\))\z/
      FOREIGN_KEY_ACTION_MAP = {0 => :no_action, 1 => :cascade, 2 => :set_null, 3 => :set_default}.freeze

      include Sequel::Database::SplitAlterTable
      
      # Whether to use N'' to quote strings, which allows unicode characters inside the
      # strings.  True by default for compatibility, can be set to false for a possible
      # performance increase.  This sets the default for all datasets created from this
      # Database object.
      attr_reader :mssql_unicode_strings

      def mssql_unicode_strings=(v)
        @mssql_unicode_strings = v
        reset_default_dataset
      end

      # The types to check for 0 scale to transform :decimal types
      # to :integer.
      DECIMAL_TYPE_RE = /number|numeric|decimal/io

      # Execute the given stored procedure with the given name.
      #
      # Options:
      # :args :: Arguments to stored procedure.  For named arguments, this should be a
      #          hash keyed by argument named.  For unnamed arguments, this should be an
      #          array.  Output parameters to the function are specified using :output.
      #          You can also name output parameters and provide a type by using an
      #          array containing :output, the type name, and the parameter name.
      # :server :: The server/shard on which to execute the procedure.
      #
      # This method returns a single hash with the following keys:
      #
      # :result :: The result code of the stored procedure
      # :numrows :: The number of rows affected by the stored procedure
      # output params :: Values for any output paramters, using the name given for the output parameter
      #
      # Examples:
      #
      #     DB.call_mssql_sproc(:SequelTest, {:args => ['input arg', :output]})
      #     DB.call_mssql_sproc(:SequelTest, {:args => ['input arg', [:output, 'int', 'varname']]})
      #
      #     named params:
      #     DB.call_mssql_sproc(:SequelTest, :args => {
      #       'input_arg1_name' => 'input arg1 value',
      #       'input_arg2_name' => 'input arg2 value',
      #       'output_arg_name' => [:output, 'int', 'varname']
      #     })
      def call_mssql_sproc(name, opts=OPTS)
        args = opts[:args] || []
        names = ['@RC AS RESULT', '@@ROWCOUNT AS NUMROWS']
        declarations = ['@RC int']
        values = []

        if args.is_a?(Hash)
          named_args = true
          args = args.to_a
          method = :each
        else
          method = :each_with_index
        end

        args.send(method) do |v, i|
          if named_args
            k = v
            v, type, select = i
            raise Error, "must provide output parameter name when using output parameters with named arguments" if v == :output && !select
          else
            v, type, select = v
          end

          if v == :output
            type ||= "nvarchar(max)"
            if named_args
              varname = select
            else
              varname = "var#{i}"
              select ||= varname
            end
            names << "@#{varname} AS #{quote_identifier(select)}"
            declarations << "@#{varname} #{type}"
            value = "@#{varname} OUTPUT"
          else
            value = literal(v)
          end

          if named_args
            value = "@#{k}=#{value}"
          end

          values << value
        end

        sql = "DECLARE #{declarations.join(', ')}; EXECUTE @RC = #{name} #{values.join(', ')}; SELECT #{names.join(', ')}"

        ds = dataset.with_sql(sql)
        ds = ds.server(opts[:server]) if opts[:server]
        ds.first
      end

      # Microsoft SQL Server uses the :mssql type.
      def database_type
        :mssql
      end
      
      # Microsoft SQL Server namespaces indexes per table.
      def global_index_namespace?
        false
      end

      # Return foreign key information using the system views, including
      # :name, :on_delete, and :on_update entries in the hashes.
      def foreign_key_list(table, opts=OPTS)
        m = output_identifier_meth
        im = input_identifier_meth
        schema, table = schema_and_table(table)
        current_schema = m.call(get(Sequel.function('schema_name')))
        fk_action_map = FOREIGN_KEY_ACTION_MAP
        ds = metadata_dataset.from(Sequel.lit('[sys].[foreign_keys]').as(:fk)).
          join(Sequel.lit('[sys].[foreign_key_columns]').as(:fkc), :constraint_object_id => :object_id).
          join(Sequel.lit('[sys].[all_columns]').as(:pc), :object_id => :fkc__parent_object_id,     :column_id => :fkc__parent_column_id).
          join(Sequel.lit('[sys].[all_columns]').as(:rc), :object_id => :fkc__referenced_object_id, :column_id => :fkc__referenced_column_id).
          where{{object_schema_name(:fk__parent_object_id) => im.call(schema || current_schema)}}.
          where{{object_name(:fk__parent_object_id) => im.call(table)}}.
          select{[:fk__name, 
                  :fk__delete_referential_action, 
                  :fk__update_referential_action, 
                  :pc__name___column, 
                  :rc__name___referenced_column, 
                  object_schema_name(:fk__referenced_object_id).as(:schema), 
                  object_name(:fk__referenced_object_id).as(:table)]}.
          order(:fk__name, :fkc__constraint_column_id)
        h = {}
        ds.each do |row|
          if r = h[row[:name]]
            r[:columns] << m.call(row[:column])
            r[:key] << m.call(row[:referenced_column])
          else
            referenced_schema = m.call(row[:schema])
            referenced_table = m.call(row[:table])
            h[row[:name]] = { :name      => m.call(row[:name]), 
                              :table     => (referenced_schema == current_schema) ? referenced_table : Sequel.qualify(referenced_schema, referenced_table),
                              :columns   => [m.call(row[:column])], 
                              :key       => [m.call(row[:referenced_column])], 
                              :on_update => fk_action_map[row[:update_referential_action]], 
                              :on_delete => fk_action_map[row[:delete_referential_action]] }
          end
        end
        h.values
      end

      # Use the system tables to get index information
      def indexes(table, opts=OPTS)
        m = output_identifier_meth
        im = input_identifier_meth
        indexes = {}
        ds = metadata_dataset.from(Sequel.lit('[sys].[tables]').as(:t)).
         join(Sequel.lit('[sys].[indexes]').as(:i), :object_id=>:object_id).
         join(Sequel.lit('[sys].[index_columns]').as(:ic), :object_id=>:object_id, :index_id=>:index_id).
         join(Sequel.lit('[sys].[columns]').as(:c), :object_id=>:object_id, :column_id=>:column_id).
         select(:i__name, :i__is_unique, :c__name___column).
         where{{t__name=>im.call(table)}}.
         where(:i__is_primary_key=>0, :i__is_disabled=>0).
         order(:i__name, :ic__index_column_id)

        if supports_partial_indexes?
          ds = ds.where(:i__has_filter=>0)
        end

        ds.each do |r|
          index = indexes[m.call(r[:name])] ||= {:columns=>[], :unique=>(r[:is_unique] && r[:is_unique]!=0)}
          index[:columns] << m.call(r[:column])
        end
        indexes
      end

      # The version of the MSSQL server, as an integer (e.g. 10001600 for
      # SQL Server 2008 Express).
      def server_version(server=nil)
        return @server_version if @server_version
        if @opts[:server_version]
          return @server_version = Integer(@opts[:server_version])
        end
        @server_version = synchronize(server) do |conn|
          (conn.server_version rescue nil) if conn.respond_to?(:server_version)
        end
        unless @server_version
          m = SERVER_VERSION_RE.match(fetch(SERVER_VERSION_SQL).single_value.to_s)
          @server_version = (m[1].to_i * 1000000) + (m[2].to_i * 10000) + m[3].to_i
        end
        @server_version
      end
        
      # MSSQL 2008+ supports partial indexes.
      def supports_partial_indexes?
        dataset.send(:is_2008_or_later?)
      end

      # MSSQL supports savepoints, though it doesn't support committing/releasing them savepoint
      def supports_savepoints?
        true
      end
      
      # MSSQL supports transaction isolation levels
      def supports_transaction_isolation_levels?
        true
      end

      # MSSQL supports transaction DDL statements.
      def supports_transactional_ddl?
        true
      end

      # Microsoft SQL Server supports using the INFORMATION_SCHEMA to get
      # information on tables.
      def tables(opts=OPTS)
        information_schema_tables('BASE TABLE', opts)
      end

      # Microsoft SQL Server supports using the INFORMATION_SCHEMA to get
      # information on views.
      def views(opts=OPTS)
        information_schema_tables('VIEW', opts)
      end
      
      private
      
      # Add dropping of the default constraint to the list of SQL queries.
      # This is necessary before dropping the column or changing its type.
      def add_drop_default_constraint_sql(sqls, table, column)
        if constraint = default_constraint_name(table, column)
          sqls << "ALTER TABLE #{quote_schema_table(table)} DROP CONSTRAINT #{constraint}"
        end
      end

      # MSSQL uses the IDENTITY(1,1) column for autoincrementing columns.
      def auto_increment_sql
        AUTO_INCREMENT
      end
      
      # MSSQL specific syntax for altering tables.
      def alter_table_sql(table, op)
        case op[:op]
        when :add_column
          "ALTER TABLE #{quote_schema_table(table)} ADD #{column_definition_sql(op)}"
        when :drop_column
          sqls = []
          add_drop_default_constraint_sql(sqls, table, op[:name])
          sqls << super
        when :rename_column
          "sp_rename #{literal("#{quote_schema_table(table)}.#{quote_identifier(op[:name])}")}, #{literal(op[:new_name].to_s)}, 'COLUMN'"
        when :set_column_type
          sqls = []
          if sch = schema(table)
            if cs = sch.each{|k, v| break v if k == op[:name]; nil}
              cs = cs.dup
              add_drop_default_constraint_sql(sqls, table, op[:name])
              cs[:default] = cs[:ruby_default]
              op = cs.merge!(op)
              default = op.delete(:default)
            end
          end
          sqls << "ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{column_definition_sql(op)}"
          sqls << alter_table_sql(table, op.merge(:op=>:set_column_default, :default=>default)) if default
          sqls
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

      # Handle MSSQL specific default format.
      def column_schema_normalize_default(default, type)
        if m = MSSQL_DEFAULT_RE.match(default)
          default = m[1] || m[2]
        end
        super(default, type)
      end

      # Commit the active transaction on the connection, does not commit/release
      # savepoints.
      def commit_transaction(conn, opts=OPTS)
        log_connection_execute(conn, commit_transaction_sql) unless savepoint_level(conn) > 1
      end

      # SQL to COMMIT a transaction.
      def commit_transaction_sql
        SQL_COMMIT
      end
        
      # MSSQL uses the name of the table to decide the difference between
      # a regular and temporary table, with temporary table names starting with
      # a #.
      def create_table_prefix_sql(name, options)
        "CREATE TABLE #{quote_schema_table(options[:temp] ? "##{name}" : name)}"
      end
      
      # MSSQL doesn't support CREATE TABLE AS, it only supports SELECT INTO.
      # Emulating CREATE TABLE AS using SELECT INTO is only possible if a dataset
      # is given as the argument, it can't work with a string, so raise an
      # Error if a string is given.
      def create_table_as(name, ds, options)
        raise(Error, "must provide dataset instance as value of create_table :as option on MSSQL") unless ds.is_a?(Sequel::Dataset)
        run(ds.into(name).sql)
      end
    
      DATABASE_ERROR_REGEXPS = {
        /Violation of UNIQUE KEY constraint|Violation of PRIMARY KEY constraint.+Cannot insert duplicate key/ => UniqueConstraintViolation,
        /conflicted with the (FOREIGN KEY.*|REFERENCE) constraint/ => ForeignKeyConstraintViolation,
        /conflicted with the CHECK constraint/ => CheckConstraintViolation,
        /column does not allow nulls/ => NotNullConstraintViolation,
        /was deadlocked on lock resources with another process and has been chosen as the deadlock victim/ => SerializationFailure,
      }.freeze
      def database_error_regexps
        DATABASE_ERROR_REGEXPS
      end

      # The name of the constraint for setting the default value on the table and column.
      # The SQL used to select default constraints utilizes MSSQL catalog views which were introduced in 2005.
      # This method intentionally does not support MSSQL 2000.
      def default_constraint_name(table, column_name)
        if server_version >= 9000000
          table_name = schema_and_table(table).compact.join('.')
          self[:sys__default_constraints].
            where{{:parent_object_id => Sequel::SQL::Function.new(:object_id, table_name), col_name(:parent_object_id, :parent_column_id) => column_name.to_s}}.
            get(:name)
        end
      end

      # The SQL to drop an index for the table.
      def drop_index_sql(table, op)
        "DROP INDEX #{quote_identifier(op[:name] || default_index_name(table, op[:columns]))} ON #{quote_schema_table(table)}"
      end
      
      # support for clustered index type
      def index_definition_sql(table_name, index)
        index_name = index[:name] || default_index_name(table_name, index[:columns])
        raise Error, "Partial indexes are not supported for this database" if index[:where] && !supports_partial_indexes?
        if index[:type] == :full_text
          "CREATE FULLTEXT INDEX ON #{quote_schema_table(table_name)} #{literal(index[:columns])} KEY INDEX #{literal(index[:key_index])}"
        else
          "CREATE #{'UNIQUE ' if index[:unique]}#{'CLUSTERED ' if index[:type] == :clustered}INDEX #{quote_identifier(index_name)} ON #{quote_schema_table(table_name)} #{literal(index[:columns])}#{" INCLUDE #{literal(index[:include])}" if index[:include]}#{" WHERE #{filter_expr(index[:where])}" if index[:where]}"
        end
      end

      # Backbone of the tables and views support.
      def information_schema_tables(type, opts)
        m = output_identifier_meth
        metadata_dataset.from(:information_schema__tables___t).
          select(:table_name).
          filter(:table_type=>type, :table_schema=>(opts[:schema]||'dbo').to_s).
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
        when /\A(?:bit)\z/io
          :boolean
        when /\A(?:(?:small)?money)\z/io
          :decimal
        when /\A(timestamp|rowversion)\z/io
          :blob
        else
          super
        end
      end

      # MSSQL uses the INFORMATION_SCHEMA to hold column information, and
      # parses primary key information from the sysindexes, sysindexkeys,
      # and syscolumns system tables.
      def schema_parse_table(table_name, opts)
        m = output_identifier_meth(opts[:dataset])
        m2 = input_identifier_meth(opts[:dataset])
        tn = m2.call(table_name.to_s)
        table_id = get(Sequel.function(:object_id, tn))
        info_sch_sch = opts[:information_schema_schema]
        inf_sch_qual = lambda{|s| info_sch_sch ? Sequel.qualify(info_sch_sch, s) : Sequel.expr(s)}
        sys_qual = lambda{|s| info_sch_sch ? Sequel.qualify(info_sch_sch, Sequel.qualify(Sequel.lit(''), s)) : Sequel.expr(s)}

        identity_cols = metadata_dataset.from(Sequel.lit('[sys].[columns]')).
          where(:object_id=>table_id, :is_identity=>true).
          select_map(:name)

        pk_index_id = metadata_dataset.from(sys_qual.call(Sequel.lit('sysindexes'))).
          where(:id=>table_id, :indid=>1..254){{(status & 2048)=>2048}}.
          get(:indid)
        pk_cols = metadata_dataset.from(sys_qual.call(Sequel.lit('sysindexkeys')).as(:sik)).
          join(sys_qual.call(Sequel.lit('syscolumns')).as(:sc), :id=>:id, :colid=>:colid).
          where(:sik__id=>table_id, :sik__indid=>pk_index_id).
          select_order_map(:sc__name)

        ds = metadata_dataset.from(inf_sch_qual.call(:information_schema__tables).as(:t)).
         join(inf_sch_qual.call(:information_schema__columns).as(:c), :table_catalog=>:table_catalog,
              :table_schema => :table_schema, :table_name => :table_name).
         select(:column_name___column, :data_type___db_type, :character_maximum_length___max_chars, :column_default___default, :is_nullable___allow_null, :numeric_precision___column_size, :numeric_scale___scale).
         filter(:c__table_name=>tn)

        if schema = opts[:schema]
          ds.filter!(:c__table_schema=>schema)
        end

        ds.map do |row|
          if row[:primary_key] = pk_cols.include?(row[:column])
            row[:auto_increment] = identity_cols.include?(row[:column])
          end
          row[:allow_null] = row[:allow_null] == 'YES' ? true : false
          row[:default] = nil if blank_object?(row[:default])
          row[:type] = if row[:db_type] =~ DECIMAL_TYPE_RE && row[:scale] == 0
            :integer
          else
            schema_column_type(row[:db_type])
          end
          row[:max_length] = row[:max_chars] if row[:type] == :string && row[:max_chars] >= 0
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
      
      # MSSQL supports views with check option, but not local.
      def view_with_check_option_support
        true
      end
    end
  
    module DatasetMethods
      include(Module.new do
        Dataset.def_sql_method(self, :select, %w'with select distinct limit columns into from lock join where group having order compounds')
      end)
      include EmulateOffsetWithRowNumber

      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      COMMA_SEPARATOR = ', '.freeze
      NOLOCK = ' WITH (NOLOCK)'.freeze
      UPDLOCK = ' WITH (UPDLOCK)'.freeze
      WILDCARD = LiteralString.new('*').freeze
      CONSTANT_MAP = {:CURRENT_DATE=>'CAST(CURRENT_TIMESTAMP AS DATE)'.freeze, :CURRENT_TIME=>'CAST(CURRENT_TIMESTAMP AS TIME)'.freeze}
      EXTRACT_MAP = {:year=>"yy", :month=>"m", :day=>"d", :hour=>"hh", :minute=>"n", :second=>"s"}
      BRACKET_CLOSE = Dataset::BRACKET_CLOSE
      BRACKET_OPEN = Dataset::BRACKET_OPEN
      COMMA = Dataset::COMMA
      PAREN_CLOSE = Dataset::PAREN_CLOSE
      PAREN_SPACE_OPEN = Dataset::PAREN_SPACE_OPEN
      SPACE = Dataset::SPACE
      FROM = Dataset::FROM
      APOS = Dataset::APOS
      APOS_RE = Dataset::APOS_RE
      DOUBLE_APOS = Dataset::DOUBLE_APOS
      INTO = Dataset::INTO
      DOUBLE_BRACKET_CLOSE = ']]'.freeze
      DATEPART_SECOND_OPEN = "CAST((datepart(".freeze
      DATEPART_SECOND_MIDDLE = ') + datepart(ns, '.freeze
      DATEPART_SECOND_CLOSE = ")/1000000000.0) AS double precision)".freeze
      DATEPART_OPEN = "datepart(".freeze
      OUTPUT_INSERTED = " OUTPUT INSERTED.*".freeze
      HEX_START = '0x'.freeze
      UNICODE_STRING_START = "N'".freeze
      BACKSLASH_CRLF_RE = /\\((?:\r\n)|\n)/.freeze
      BACKSLASH_CRLF_REPLACE = '\\\\\\\\\\1\\1'.freeze
      TOP_PAREN = " TOP (".freeze
      TOP = " TOP ".freeze
      OUTPUT = " OUTPUT ".freeze
      HSTAR = "H*".freeze
      CASE_SENSITIVE_COLLATION = 'Latin1_General_CS_AS'.freeze
      CASE_INSENSITIVE_COLLATION = 'Latin1_General_CI_AS'.freeze
      DEFAULT_TIMESTAMP_FORMAT = "'%Y-%m-%dT%H:%M:%S%N%z'".freeze
      FORMAT_DATE = "'%Y%m%d'".freeze
      CROSS_APPLY = 'CROSS APPLY'.freeze
      OUTER_APPLY = 'OUTER APPLY'.freeze
      OFFSET = " OFFSET ".freeze
      ROWS = " ROWS".freeze
      ROWS_ONLY = " ROWS ONLY".freeze
      FETCH_NEXT = " FETCH NEXT ".freeze

      Dataset.def_mutation_method(:disable_insert_output, :output, :module=>self)
      Dataset.def_sql_method(self, :delete, %w'with delete from output from2 where')
      Dataset.def_sql_method(self, :insert, %w'with insert into columns output values')
      Dataset.def_sql_method(self, :update, [['if is_2005_or_later?', %w'with update limit table set output from where'], ['else', %w'update table set output from where']])


      # Allow overriding of the mssql_unicode_strings option at the dataset level.
      attr_writer :mssql_unicode_strings

      # Use the database's mssql_unicode_strings setting if the dataset hasn't overridden it.
      def mssql_unicode_strings
        defined?(@mssql_unicode_strings) ? @mssql_unicode_strings : (@mssql_unicode_strings = db.mssql_unicode_strings)
      end

      # MSSQL uses + for string concatenation, and LIKE is case insensitive by default.
      def complex_expression_sql_append(sql, op, args)
        case op
        when :'||'
          super(sql, :+, args)
        when :LIKE, :"NOT LIKE"
          super(sql, op, args.map{|a| Sequel.lit(["(", " COLLATE #{CASE_SENSITIVE_COLLATION})"], a)})
        when :ILIKE, :"NOT ILIKE"
          super(sql, (op == :ILIKE ? :LIKE : :"NOT LIKE"), args.map{|a| Sequel.lit(["(", " COLLATE #{CASE_INSENSITIVE_COLLATION})"], a)})
        when :<<, :>>
          complex_expression_emulate_append(sql, op, args)
        when :extract
          part = args.at(0)
          raise(Sequel::Error, "unsupported extract argument: #{part.inspect}") unless format = EXTRACT_MAP[part]
          if part == :second
            expr = args.at(1)
            sql << DATEPART_SECOND_OPEN << format.to_s << COMMA
            literal_append(sql, expr)
            sql << DATEPART_SECOND_MIDDLE
            literal_append(sql, expr)
            sql << DATEPART_SECOND_CLOSE
          else
            sql << DATEPART_OPEN << format.to_s << COMMA
            literal_append(sql, args.at(1))
            sql << PAREN_CLOSE
          end
        else
          super
        end
      end
      
      # MSSQL doesn't support the SQL standard CURRENT_DATE or CURRENT_TIME
      def constant_sql_append(sql, constant)
        if c = CONSTANT_MAP[constant]
          sql << c
        else
          super
        end
      end
      
      # Uses CROSS APPLY to join the given table into the current dataset.
      def cross_apply(table)
        join_table(:cross_apply, table)
      end

      # Disable the use of INSERT OUTPUT
      def disable_insert_output
        clone(:disable_insert_output=>true)
      end

      # MSSQL treats [] as a metacharacter in LIKE expresions.
      def escape_like(string)
        string.gsub(/[\\%_\[\]]/){|m| "\\#{m}"}
      end
   
      # MSSQL uses the CONTAINS keyword for full text search
      def full_text_search(cols, terms, opts = OPTS)
        terms = "\"#{terms.join('" OR "')}\"" if terms.is_a?(Array)
        filter("CONTAINS (?, ?)", cols, terms)
      end

      # Use the OUTPUT clause to get the value of all columns for the newly inserted record.
      def insert_select(*values)
        return unless supports_insert_select?
        with_sql_first(insert_select_sql(*values))
      end

      # Add OUTPUT clause unless there is already an existing output clause, then return
      # the SQL to insert.
      def insert_select_sql(*values)
        ds = (opts[:output] || opts[:returning]) ? self : output(nil, [SQL::ColumnAll.new(:inserted)])
        ds.insert_sql(*values)
      end

      # Specify a table for a SELECT ... INTO query.
      def into(table)
        clone(:into => table)
      end

      # Allows you to do a dirty read of uncommitted data using WITH (NOLOCK).
      def nolock
        lock_style(:dirty)
      end

      # Uses OUTER APPLY to join the given table into the current dataset.
      def outer_apply(table)
        join_table(:outer_apply, table)
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
        clone(:output => output)
      end

      # MSSQL uses [] to quote identifiers.
      def quoted_identifier_append(sql, name)
        sql << BRACKET_OPEN << name.to_s.gsub(/\]/, DOUBLE_BRACKET_CLOSE) << BRACKET_CLOSE
      end

      # Emulate RETURNING using the output clause.  This only handles values that are simple column references.
      def returning(*values)
        values = values.map do |v|
          unless r = unqualified_column_for(v)
            raise(Error, "cannot emulate RETURNING via OUTPUT for value: #{v.inspect}")
          end
          r
        end
        clone(:returning=>values)
      end

      # On MSSQL 2012+ add a default order to the current dataset if an offset is used.
      # The default offset emulation using a subquery would be used in the unordered
      # case by default, and that also adds a default order, so it's better to just
      # avoid the subquery.
      def select_sql
        if @opts[:offset] && !@opts[:order] && is_2012_or_later?
          order(1).select_sql
        else
          super
        end
      end

      # The version of the database server.
      def server_version
        db.server_version(@opts[:server])
      end

      def supports_cte?(type=:select)
        is_2005_or_later?
      end

      # MSSQL 2005+ supports GROUP BY CUBE.
      def supports_group_cube?
        is_2005_or_later?
      end

      # MSSQL 2005+ supports GROUP BY ROLLUP
      def supports_group_rollup?
        is_2005_or_later?
      end

      # MSSQL 2005+ supports GROUPING SETS
      def supports_grouping_sets?
        is_2008_or_later?
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
      
      # MSSQL 2012+ supports offsets in correlated subqueries.
      def supports_offsets_in_correlated_subqueries?
        is_2012_or_later?
      end

      # MSSQL 2005+ supports the OUTPUT clause.
      def supports_output_clause?
        is_2005_or_later?
      end

      # MSSQL 2005+ can emulate RETURNING via the OUTPUT clause.
      def supports_returning?(type)
        supports_insert_select?
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
      
      # If returned primary keys are requested, use OUTPUT unless already set on the
      # dataset.  If OUTPUT is already set, use existing returning values.  If OUTPUT
      # is only set to return a single columns, return an array of just that column.
      # Otherwise, return an array of hashes.
      def _import(columns, values, opts=OPTS)
        if opts[:return] == :primary_key && !@opts[:output]
          output(nil, [SQL::QualifiedIdentifier.new(:inserted, first_primary_key)])._import(columns, values, opts)
        elsif @opts[:output]
          statements = multi_insert_sql(columns, values)
          @db.transaction(opts.merge(:server=>@opts[:server])) do
            statements.map{|st| with_sql(st)}
          end.first.map{|v| v.length == 1 ? v.values.first : v}
        else
          super
        end
      end

      # MSSQL does not allow ordering in sub-clauses unless 'top' (limit) is specified
      def aggregate_dataset
        (options_overlap(Sequel::Dataset::COUNT_FROM_SELF_OPTS) && !options_overlap([:limit])) ? unordered.from_self : super
      end

      private

      # Whether we are using SQL Server 2005 or later.
      def is_2005_or_later?
        server_version >= 9000000
      end

      # Whether we are using SQL Server 2008 or later.
      def is_2008_or_later?
        server_version >= 10000000
      end

      # Whether we are using SQL Server 2012 or later.
      def is_2012_or_later?
        server_version >= 11000000
      end

      # Use strict ISO-8601 format with T between date and time,
      # since that is the format that is multilanguage and not
      # DATEFORMAT dependent.
      def default_timestamp_format
        DEFAULT_TIMESTAMP_FORMAT
      end

      # Only include the primary table in the main delete clause
      def delete_from_sql(sql)
        sql << FROM
        source_list_append(sql, @opts[:from][0..0])
      end

      # MSSQL supports FROM clauses in DELETE and UPDATE statements.
      def delete_from2_sql(sql)
        if joined_dataset?
          select_from_sql(sql)
          select_join_sql(sql)
        end
      end
      alias update_from_sql delete_from2_sql

      def delete_output_sql(sql)
        output_sql(sql, :DELETED)
      end

      # There is no function on Microsoft SQL Server that does character length
      # and respects trailing spaces (datalength respects trailing spaces, but
      # counts bytes instead of characters).  Use a hack to work around the
      # trailing spaces issue.
      def emulate_function?(name)
        name == :char_length || name == :trim
      end

      def emulate_function_sql_append(sql, f)
        case f.name
        when :char_length
          literal_append(sql, SQL::Function.new(:len, Sequel.join([f.args.first, 'x'])) - 1)
        when :trim
          literal_append(sql, SQL::Function.new(:ltrim, SQL::Function.new(:rtrim, f.args.first)))
        end
      end
      
      # Microsoft SQL Server 2012 has native support for offsets, but only for ordered datasets.
      def emulate_offset_with_row_number?
        super && !(is_2012_or_later? && @opts[:order])
      end
      
      # Return the first primary key for the current table.  If this table has
      # multiple primary keys, this will only return one of them.  Used by #_import.
      def first_primary_key
        @db.schema(self).map{|k, v| k if v[:primary_key] == true}.compact.first
      end

      def insert_output_sql(sql)
        output_sql(sql, :INSERTED)
      end
      alias update_output_sql insert_output_sql

      # Handle CROSS APPLY and OUTER APPLY JOIN types
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

      # MSSQL uses a literal hexidecimal number for blob strings
      def literal_blob_append(sql, v)
        sql << HEX_START << v.unpack(HSTAR).first
      end
      
      # Use YYYYmmdd format, since that's the only want that is
      # multilanguage and not DATEFORMAT dependent.
      def literal_date(v)
        v.strftime(FORMAT_DATE)
      end

      # Use 0 for false on MSSQL
      def literal_false
        BOOL_FALSE
      end

      # Optionally use unicode string syntax for all strings. Don't double
      # backslashes.
      def literal_string_append(sql, v)
        sql << (mssql_unicode_strings ? UNICODE_STRING_START : APOS)
        sql << v.gsub(APOS_RE, DOUBLE_APOS).gsub(BACKSLASH_CRLF_RE, BACKSLASH_CRLF_REPLACE) << APOS
      end
      
      # Use 1 for true on MSSQL
      def literal_true
        BOOL_TRUE
      end
      
      # MSSQL 2008+ supports multiple rows in the VALUES clause, older versions
      # can use UNION.
      def multi_insert_sql_strategy
        is_2008_or_later? ? :values : :union
      end

      def select_into_sql(sql)
        if i = @opts[:into]
          sql << INTO
          identifier_append(sql, i)
        end
      end

      # MSSQL uses TOP N for limit.  For MSSQL 2005+ TOP (N) is used
      # to allow the limit to be a bound variable.
      def select_limit_sql(sql)
        if l = @opts[:limit]
          return if is_2012_or_later? && @opts[:order] && @opts[:offset]

          if is_2005_or_later?
            sql << TOP_PAREN
            literal_append(sql, l)
            sql << PAREN_CLOSE
          else
            sql << TOP
            literal_append(sql, l)
          end
        end
      end
      alias update_limit_sql select_limit_sql

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

      # On 2012+ when there is an order with an offset, append the offset (and possible
      # limit) at the end of the order clause.
      def select_order_sql(sql)
        super
        if is_2012_or_later? && @opts[:order]
          if o = @opts[:offset]
            sql << OFFSET
            literal_append(sql, o)
            sql << ROWS

            if l = @opts[:limit]
              sql << FETCH_NEXT
              literal_append(sql, l)
              sql << ROWS_ONLY
            end
          end
        end
      end

      # SQL fragment for MSSQL's OUTPUT clause.
      def output_sql(sql, type)
        return unless supports_output_clause?
        if output = @opts[:output]
          output_list_sql(sql, output)
        elsif values = @opts[:returning]
          output_returning_sql(sql, type, values)
        end
      end

      def output_list_sql(sql, output)
        sql << OUTPUT
        column_list_append(sql, output[:select_list])
        if into = output[:into]
          sql << INTO
          identifier_append(sql, into)
          if column_list = output[:column_list]
            sql << PAREN_SPACE_OPEN
            source_list_append(sql, column_list)
            sql << PAREN_CLOSE
          end
        end
      end

      def output_returning_sql(sql, type, values)
        sql << OUTPUT
        if values.empty?
          literal_append(sql, SQL::ColumnAll.new(type))
        else
          values = values.map do |v|
            case v
            when SQL::AliasedExpression
              Sequel.qualify(type, v.expression).as(v.alias)
            else
              Sequel.qualify(type, v)
            end
          end
          column_list_append(sql, values)
        end
      end

      # MSSQL supports millisecond timestamp precision.
      def timestamp_precision
        3
      end

      # Only include the primary table in the main update clause
      def update_table_sql(sql)
        sql << SPACE
        source_list_append(sql, @opts[:from][0..0])
      end

      def uses_with_rollup?
        !is_2008_or_later?
      end
    end
  end
end
