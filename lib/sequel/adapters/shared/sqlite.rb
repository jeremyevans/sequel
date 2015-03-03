Sequel.require 'adapters/utils/replace'

module Sequel
  module SQLite
    # No matter how you connect to SQLite, the following Database options
    # can be used to set PRAGMAs on connections in a thread-safe manner:
    # :auto_vacuum, :foreign_keys, :synchronous, and :temp_store.
    module DatabaseMethods
      extend Sequel::Database::ResetIdentifierMangling

      AUTO_VACUUM = [:none, :full, :incremental].freeze
      PRIMARY_KEY_INDEX_RE = /\Asqlite_autoindex_/.freeze
      SYNCHRONOUS = [:off, :normal, :full].freeze
      TABLES_FILTER = "type = 'table' AND NOT name = 'sqlite_sequence'".freeze
      TEMP_STORE = [:default, :file, :memory].freeze
      VIEWS_FILTER = "type = 'view'".freeze
      TRANSACTION_MODE = {
        :deferred => "BEGIN DEFERRED TRANSACTION".freeze,
        :immediate => "BEGIN IMMEDIATE TRANSACTION".freeze,
        :exclusive => "BEGIN EXCLUSIVE TRANSACTION".freeze,
        nil => Sequel::Database::SQL_BEGIN,
      }.freeze

      # Whether to use integers for booleans in the database.  SQLite recommends
      # booleans be stored as integers, but historically Sequel has used 't'/'f'.
      attr_accessor :integer_booleans

      # A symbol signifying the value of the auto_vacuum PRAGMA.
      def auto_vacuum
        AUTO_VACUUM[pragma_get(:auto_vacuum).to_i]
      end
      
      # Set the auto_vacuum PRAGMA using the given symbol (:none, :full, or
      # :incremental).  See pragma_set.  Consider using the :auto_vacuum
      # Database option instead.
      def auto_vacuum=(value)
        value = AUTO_VACUUM.index(value) || (raise Error, "Invalid value for auto_vacuum option. Please specify one of :none, :full, :incremental.")
        pragma_set(:auto_vacuum, value)
      end

      # Set the case_sensitive_like PRAGMA using the given boolean value, if using
      # SQLite 3.2.3+.  If not using 3.2.3+, no error is raised. See pragma_set.
      # Consider using the :case_sensitive_like Database option instead.
      def case_sensitive_like=(value)
        pragma_set(:case_sensitive_like, !!value ? 'on' : 'off') if sqlite_version >= 30203
      end

      # A symbol signifying the value of the default transaction mode
      attr_reader :transaction_mode

      # Set the default transaction mode.
      def transaction_mode=(value)
        if TRANSACTION_MODE.include?(value)
          @transaction_mode = value
        else
          raise Error, "Invalid value for transaction_mode.  Please specify one of :deferred, :immediate, :exclusive, nil"
        end
      end

      # SQLite uses the :sqlite database type.
      def database_type
        :sqlite
      end
      
      # Boolean signifying the value of the foreign_keys PRAGMA, or nil
      # if not using SQLite 3.6.19+.
      def foreign_keys
        pragma_get(:foreign_keys).to_i == 1 if sqlite_version >= 30619
      end
      
      # Set the foreign_keys PRAGMA using the given boolean value, if using
      # SQLite 3.6.19+.  If not using 3.6.19+, no error is raised. See pragma_set.
      # Consider using the :foreign_keys Database option instead.
      def foreign_keys=(value)
        pragma_set(:foreign_keys, !!value ? 'on' : 'off') if sqlite_version >= 30619
      end

      # Return the array of foreign key info hashes using the foreign_key_list PRAGMA,
      # including information for the :on_update and :on_delete entries.
      def foreign_key_list(table, opts=OPTS)
        m = output_identifier_meth
        h = {}
        metadata_dataset.with_sql("PRAGMA foreign_key_list(?)", input_identifier_meth.call(table)).each do |row|
          if r = h[row[:id]]
            r[:columns] << m.call(row[:from])
            r[:key] << m.call(row[:to]) if r[:key]
          else
            h[row[:id]] = {:columns=>[m.call(row[:from])], :table=>m.call(row[:table]), :key=>([m.call(row[:to])] if row[:to]), :on_update=>on_delete_sql_to_sym(row[:on_update]), :on_delete=>on_delete_sql_to_sym(row[:on_delete])}
          end
        end
        h.values
      end

      # Use the index_list and index_info PRAGMAs to determine the indexes on the table.
      def indexes(table, opts=OPTS)
        m = output_identifier_meth
        im = input_identifier_meth
        indexes = {}
        metadata_dataset.with_sql("PRAGMA index_list(?)", im.call(table)).each do |r|
          # :only_autocreated internal option can be used to get only autocreated indexes
          next if (!!(r[:name] =~ PRIMARY_KEY_INDEX_RE) ^ !!opts[:only_autocreated])
          indexes[m.call(r[:name])] = {:unique=>r[:unique].to_i==1}
        end
        indexes.each do |k, v|
          v[:columns] = metadata_dataset.with_sql("PRAGMA index_info(?)", im.call(k)).map(:name).map{|x| m.call(x)}
        end
        indexes
      end

      # Get the value of the given PRAGMA.
      def pragma_get(name)
        self["PRAGMA #{name}"].single_value
      end
      
      # Set the value of the given PRAGMA to value.
      #
      # This method is not thread safe, and will not work correctly if there
      # are multiple connections in the Database's connection pool. PRAGMA
      # modifications should be done when the connection is created, using
      # an option provided when creating the Database object.
      def pragma_set(name, value)
        execute_ddl("PRAGMA #{name} = #{value}")
      end

      # Set the integer_booleans option using the passed in :integer_boolean option.
      def set_integer_booleans
        @integer_booleans = @opts.has_key?(:integer_booleans) ? typecast_value_boolean(@opts[:integer_booleans]) : true
      end
      
      # The version of the server as an integer, where 3.6.19 = 30619.
      # If the server version can't be determined, 0 is used.
      def sqlite_version
        return @sqlite_version if defined?(@sqlite_version)
        @sqlite_version = begin
          v = fetch('SELECT sqlite_version()').single_value
          [10000, 100, 1].zip(v.split('.')).inject(0){|a, m| a + m[0] * Integer(m[1])}
        rescue
          0
        end
      end
      
      # SQLite supports CREATE TABLE IF NOT EXISTS syntax since 3.3.0.
      def supports_create_table_if_not_exists?
        sqlite_version >= 30300
      end
      
      # SQLite 3.6.19+ supports deferrable foreign key constraints.
      def supports_deferrable_foreign_key_constraints?
        sqlite_version >= 30619
      end

      # SQLite 3.8.0+ supports partial indexes.
      def supports_partial_indexes?
        sqlite_version >= 30800
      end

      # SQLite 3.6.8+ supports savepoints. 
      def supports_savepoints?
        sqlite_version >= 30608
      end

      # Override the default setting for whether to use timezones in timestamps.
      # It is set to +false+ by default, as SQLite's date/time methods do not
      # support timezones in timestamps.
      attr_writer :use_timestamp_timezones

      # SQLite supports timezones in timestamps, since it just stores them as strings,
      # but it breaks the usage of SQLite's datetime functions.
      def use_timestamp_timezones?
        defined?(@use_timestamp_timezones) ? @use_timestamp_timezones : (@use_timestamp_timezones = false)
      end

      # A symbol signifying the value of the synchronous PRAGMA.
      def synchronous
        SYNCHRONOUS[pragma_get(:synchronous).to_i]
      end
      
      # Set the synchronous PRAGMA using the given symbol (:off, :normal, or :full). See pragma_set.
      # Consider using the :synchronous Database option instead.
      def synchronous=(value)
        value = SYNCHRONOUS.index(value) || (raise Error, "Invalid value for synchronous option. Please specify one of :off, :normal, :full.")
        pragma_set(:synchronous, value)
      end
      
      # Array of symbols specifying the table names in the current database.
      #
      # Options:
      # :server :: Set the server to use.
      def tables(opts=OPTS)
        tables_and_views(TABLES_FILTER, opts)
      end
      
      # A symbol signifying the value of the temp_store PRAGMA.
      def temp_store
        TEMP_STORE[pragma_get(:temp_store).to_i]
      end
      
      # Set the temp_store PRAGMA using the given symbol (:default, :file, or :memory). See pragma_set.
      # Consider using the :temp_store Database option instead.
      def temp_store=(value)
        value = TEMP_STORE.index(value) || (raise Error, "Invalid value for temp_store option. Please specify one of :default, :file, :memory.")
        pragma_set(:temp_store, value)
      end
      
      # Array of symbols specifying the view names in the current database.
      #
      # Options:
      # :server :: Set the server to use.
      def views(opts=OPTS)
        tables_and_views(VIEWS_FILTER, opts)
      end

      private

      # Run all alter_table commands in a transaction.  This is technically only
      # needed for drop column.
      def apply_alter_table(table, ops)
        fks = foreign_keys
        self.foreign_keys = false if fks
        transaction do 
          if ops.length > 1 && ops.all?{|op| op[:op] == :add_constraint}
            # If you are just doing constraints, apply all of them at the same time,
            # as otherwise all but the last one get lost.
            alter_table_sql_list(table, [{:op=>:add_constraints, :ops=>ops}]).flatten.each{|sql| execute_ddl(sql)}
          else
            # Run each operation separately, as later operations may depend on the
            # results of earlier operations.
            ops.each{|op| alter_table_sql_list(table, [op]).flatten.each{|sql| execute_ddl(sql)}}
          end
        end
      ensure
        self.foreign_keys = true if fks
      end

      # SQLite supports limited table modification.  You can add a column
      # or an index.  Dropping columns is supported by copying the table into
      # a temporary table, dropping the table, and creating a new table without
      # the column inside of a transaction.
      def alter_table_sql(table, op)
        case op[:op]
        when :add_index, :drop_index
          super
        when :add_column
          if op[:unique] || op[:primary_key]
            duplicate_table(table){|columns| columns.push(op)}
          else
            super
          end
        when :drop_column
          ocp = lambda{|oc| oc.delete_if{|c| c.to_s == op[:name].to_s}}
          duplicate_table(table, :old_columns_proc=>ocp){|columns| columns.delete_if{|s| s[:name].to_s == op[:name].to_s}}
        when :rename_column
          ncp = lambda{|nc| nc.map!{|c| c.to_s == op[:name].to_s ? op[:new_name] : c}}
          duplicate_table(table, :new_columns_proc=>ncp){|columns| columns.each{|s| s[:name] = op[:new_name] if s[:name].to_s == op[:name].to_s}}
        when :set_column_default
          duplicate_table(table){|columns| columns.each{|s| s[:default] = op[:default] if s[:name].to_s == op[:name].to_s}}
        when :set_column_null
          duplicate_table(table){|columns| columns.each{|s| s[:null] = op[:null] if s[:name].to_s == op[:name].to_s}}
        when :set_column_type
          duplicate_table(table){|columns| columns.each{|s| s.merge!(op) if s[:name].to_s == op[:name].to_s}}
        when :drop_constraint
          case op[:type]
          when :primary_key
            duplicate_table(table){|columns| columns.each{|s| s[:primary_key] = nil}}
          when :foreign_key
            if op[:columns]
              duplicate_table(table, :skip_foreign_key_columns=>op[:columns])
            else
              duplicate_table(table, :no_foreign_keys=>true)
            end
          else
            duplicate_table(table)
          end
        when :add_constraint
          duplicate_table(table, :constraints=>[op])
        when :add_constraints
          duplicate_table(table, :constraints=>op[:ops])
        else
          raise Error, "Unsupported ALTER TABLE operation: #{op[:op].inspect}"
        end
      end

      def begin_new_transaction(conn, opts)
        mode = opts[:mode] || @transaction_mode
        sql = TRANSACTION_MODE[mode] or raise Error, "transaction :mode must be one of: :deferred, :immediate, :exclusive, nil"
        log_connection_execute(conn, sql)
        set_transaction_isolation(conn, opts)
      end

      # A name to use for the backup table
      def backup_table_name(table, opts=OPTS)
        table = table.gsub('`', '')
        (opts[:times]||1000).times do |i|
          table_name = "#{table}_backup#{i}"
          return table_name unless table_exists?(table_name)
        end
      end

      # Surround default with parens to appease SQLite
      def column_definition_default_sql(sql, column)
        sql << " DEFAULT (#{literal(column[:default])})" if column.include?(:default)
      end
    
      # Add null/not null SQL fragment to column creation SQL.
      def column_definition_null_sql(sql, column)
        column = column.merge(:null=>false) if column[:primary_key]
        super(sql, column)
      end
    
      # Array of PRAGMA SQL statements based on the Database options that should be applied to
      # new connections.
      def connection_pragmas
        ps = []
        v = typecast_value_boolean(opts.fetch(:foreign_keys, 1))
        ps << "PRAGMA foreign_keys = #{v ? 1 : 0}"
        v = typecast_value_boolean(opts.fetch(:case_sensitive_like, 1))
        ps << "PRAGMA case_sensitive_like = #{v ? 1 : 0}"
        [[:auto_vacuum, AUTO_VACUUM], [:synchronous, SYNCHRONOUS], [:temp_store, TEMP_STORE]].each do |prag, con|
          if v = opts[prag]
            raise(Error, "Value for PRAGMA #{prag} not supported, should be one of #{con.join(', ')}") unless v = con.index(v.to_sym)
            ps << "PRAGMA #{prag} = #{v}"
          end
        end
        ps
      end

      # SQLite support creating temporary views.
      def create_view_prefix_sql(name, options)
        "CREATE #{'TEMPORARY 'if options[:temp]}VIEW #{quote_schema_table(name)}"
      end

      DATABASE_ERROR_REGEXPS = {
        /(is|are) not unique\z|PRIMARY KEY must be unique\z|UNIQUE constraint failed: .+\z/ => UniqueConstraintViolation,
        /foreign key constraint failed\z/i => ForeignKeyConstraintViolation,
        /\ACHECK constraint failed/ => CheckConstraintViolation,
        /\A(SQLITE ERROR 19 \(CONSTRAINT\) : )?constraint failed\z/ => ConstraintViolation,
        /may not be NULL\z|NOT NULL constraint failed: .+\z/ => NotNullConstraintViolation,
      }.freeze
      def database_error_regexps
        DATABASE_ERROR_REGEXPS
      end

      # The array of column schema hashes for the current columns in the table
      def defined_columns_for(table)
        cols = parse_pragma(table, {})
        cols.each do |c|
          c[:default] = LiteralString.new(c[:default]) if c[:default]
          c[:type] = c[:db_type]
          c.delete(:auto_increment)
        end
        cols
      end

      # Duplicate an existing table by creating a new table, copying all records
      # from the existing table into the new table, deleting the existing table
      # and renaming the new table to the existing table's name.
      def duplicate_table(table, opts=OPTS)
        remove_cached_schema(table)
        def_columns = defined_columns_for(table)
        old_columns = def_columns.map{|c| c[:name]}
        opts[:old_columns_proc].call(old_columns) if opts[:old_columns_proc]

        yield def_columns if block_given?

        constraints = (opts[:constraints] || []).dup
        pks = []
        def_columns.each{|c| pks << c[:name] if c[:primary_key]}
        if pks.length > 1
          constraints << {:type=>:primary_key, :columns=>pks}
          def_columns.each{|c| c[:primary_key] = false if c[:primary_key]}
        end

        # If dropping a foreign key constraint, drop all foreign key constraints,
        # as there is no way to determine which one to drop.
        unless opts[:no_foreign_keys]
          fks = foreign_key_list(table)

          # If dropping a column, if there is a foreign key with that
          # column, don't include it when building a copy of the table.
          if ocp = opts[:old_columns_proc]
            fks.delete_if{|c| ocp.call(c[:columns].dup) != c[:columns]}
          end
          
          # Skip any foreign key columns where a constraint for those
          # foreign keys is being dropped.
          if sfkc = opts[:skip_foreign_key_columns]
            fks.delete_if{|c| c[:columns] == sfkc}
          end

          constraints.concat(fks.each{|h| h[:type] = :foreign_key})
        end

        # Determine unique constraints and make sure the new columns have them
        unique_columns = []
        indexes(table, :only_autocreated=>true).each_value do |h|
          unique_columns.concat(h[:columns]) if h[:columns].length == 1 && h[:unique]
        end
        unique_columns -= pks
        unless unique_columns.empty?
          unique_columns.map!{|c| quote_identifier(c)}
          def_columns.each do |c|
            c[:unique] = true if unique_columns.include?(quote_identifier(c[:name]))
          end
        end
        
        def_columns_str = (def_columns.map{|c| column_definition_sql(c)} + constraints.map{|c| constraint_definition_sql(c)}).join(', ')
        new_columns = old_columns.dup
        opts[:new_columns_proc].call(new_columns) if opts[:new_columns_proc]

        qt = quote_schema_table(table)
        bt = quote_identifier(backup_table_name(qt))
        a = [
           "ALTER TABLE #{qt} RENAME TO #{bt}",
           "CREATE TABLE #{qt}(#{def_columns_str})",
           "INSERT INTO #{qt}(#{dataset.send(:identifier_list, new_columns)}) SELECT #{dataset.send(:identifier_list, old_columns)} FROM #{bt}",
           "DROP TABLE #{bt}"
        ]
        indexes(table).each do |name, h|
          if (h[:columns].map{|x| x.to_s} - new_columns).empty?
            a << alter_table_sql(table, h.merge(:op=>:add_index, :name=>name))
          end
        end
        a
      end

      # SQLite folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on input.
      def identifier_input_method_default
        nil
      end
      
      # SQLite folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on output.
      def identifier_output_method_default
        nil
      end
      
      # Does the reverse of on_delete_clause, eg. converts strings like +'SET NULL'+
      # to symbols +:set_null+.
      def on_delete_sql_to_sym str
        case str
        when 'RESTRICT'
          :restrict
        when 'CASCADE'
          :cascade
        when 'SET NULL'
          :set_null
        when 'SET DEFAULT'
          :set_default
        when 'NO ACTION'
          :no_action
        end
      end

      # Parse the output of the table_info pragma
      def parse_pragma(table_name, opts)
        metadata_dataset.with_sql("PRAGMA table_info(?)", input_identifier_meth(opts[:dataset]).call(table_name)).map do |row|
          row.delete(:cid)
          row[:allow_null] = row.delete(:notnull).to_i == 0
          row[:default] = row.delete(:dflt_value)
          row[:default] = nil if blank_object?(row[:default]) || row[:default] == 'NULL'
          row[:db_type] = row.delete(:type)
          if row[:primary_key] = row.delete(:pk).to_i > 0
            row[:auto_increment] = row[:db_type].downcase == 'integer'
          end
          row[:type] = schema_column_type(row[:db_type])
          row
        end
      end
      
      # SQLite supports schema parsing using the table_info PRAGMA, so
      # parse the output of that into the format Sequel expects.
      def schema_parse_table(table_name, opts)
        m = output_identifier_meth(opts[:dataset])
        parse_pragma(table_name, opts).map do |row|
          [m.call(row.delete(:name)), row]
        end
      end
      
      # Backbone of the tables and views support.
      def tables_and_views(filter, opts)
        m = output_identifier_meth
        metadata_dataset.from(:sqlite_master).server(opts[:server]).filter(filter).map{|r| m.call(r[:name])}
      end

      # SQLite only supports AUTOINCREMENT on integer columns, not
      # bigint columns, so use integer instead of bigint for those
      # columns.
      def type_literal_generic_bignum(column)
        column[:auto_increment] ? :integer : super
      end
    end
    
    # Instance methods for datasets that connect to an SQLite database
    module DatasetMethods
      include Dataset::Replace

      CONSTANT_MAP = {:CURRENT_DATE=>"date(CURRENT_TIMESTAMP, 'localtime')".freeze, :CURRENT_TIMESTAMP=>"datetime(CURRENT_TIMESTAMP, 'localtime')".freeze, :CURRENT_TIME=>"time(CURRENT_TIMESTAMP, 'localtime')".freeze}
      EMULATED_FUNCTION_MAP = {:char_length=>'length'.freeze}
      EXTRACT_MAP = {:year=>"'%Y'", :month=>"'%m'", :day=>"'%d'", :hour=>"'%H'", :minute=>"'%M'", :second=>"'%f'"}
      NOT_SPACE = Dataset::NOT_SPACE
      COMMA = Dataset::COMMA
      PAREN_CLOSE = Dataset::PAREN_CLOSE
      AS = Dataset::AS
      APOS = Dataset::APOS
      EXTRACT_OPEN = "CAST(strftime(".freeze
      EXTRACT_CLOSE = ') AS '.freeze
      NUMERIC = 'NUMERIC'.freeze
      INTEGER = 'INTEGER'.freeze
      BACKTICK = '`'.freeze
      BACKTICK_RE = /`/.freeze
      DOUBLE_BACKTICK = '``'.freeze
      BLOB_START = "X'".freeze
      HSTAR = "H*".freeze
      DATE_OPEN = "date(".freeze
      DATETIME_OPEN = "datetime(".freeze
      ONLY_OFFSET = " LIMIT -1 OFFSET ".freeze

      Dataset.def_sql_method(self, :delete, [['if db.sqlite_version >= 30803', %w'with delete from where'], ["else", %w'delete from where']])
      Dataset.def_sql_method(self, :insert, [['if db.sqlite_version >= 30803', %w'with insert into columns values'], ["else", %w'insert into columns values']])
      Dataset.def_sql_method(self, :update, [['if db.sqlite_version >= 30803', %w'with update table set where'], ["else", %w'update table set where']])

      def cast_sql_append(sql, expr, type)
        if type == Time or type == DateTime
          sql << DATETIME_OPEN
          literal_append(sql, expr)
          sql << PAREN_CLOSE
        elsif type == Date
          sql << DATE_OPEN
          literal_append(sql, expr)
          sql << PAREN_CLOSE
        else
          super
        end
      end

      # SQLite doesn't support a NOT LIKE b, you need to use NOT (a LIKE b).
      # It doesn't support xor or the extract function natively, so those have to be emulated.
      def complex_expression_sql_append(sql, op, args)
        case op
        when :"NOT LIKE", :"NOT ILIKE"
          sql << NOT_SPACE
          complex_expression_sql_append(sql, (op == :"NOT ILIKE" ? :ILIKE : :LIKE), args)
        when :^
          complex_expression_arg_pairs_append(sql, args){|a, b| Sequel.lit(["((~(", " & ", ")) & (", " | ", "))"], a, b, a, b)}
        when :extract
          part = args.at(0)
          raise(Sequel::Error, "unsupported extract argument: #{part.inspect}") unless format = EXTRACT_MAP[part]
          sql << EXTRACT_OPEN << format << COMMA
          literal_append(sql, args.at(1))
          sql << EXTRACT_CLOSE << (part == :second ? NUMERIC : INTEGER) << PAREN_CLOSE
        else
          super
        end
      end
      
      # SQLite has CURRENT_TIMESTAMP and related constants in UTC instead
      # of in localtime, so convert those constants to local time.
      def constant_sql_append(sql, constant)
        if c = CONSTANT_MAP[constant]
          sql << c
        else
          super
        end
      end
      
      # SQLite performs a TRUNCATE style DELETE if no filter is specified.
      # Since we want to always return the count of records, add a condition
      # that is always true and then delete.
      def delete
        @opts[:where] ? super : where(1=>1).delete
      end
      
      # Return an array of strings specifying a query explanation for a SELECT of the
      # current dataset. Currently, the options are ignore, but it accepts options
      # to be compatible with other adapters.
      def explain(opts=nil)
        # Load the PrettyTable class, needed for explain output
        Sequel.extension(:_pretty_table) unless defined?(Sequel::PrettyTable)

        ds = db.send(:metadata_dataset).clone(:sql=>"EXPLAIN #{select_sql}")
        rows = ds.all
        Sequel::PrettyTable.string(rows, ds.columns)
      end
      
      # HAVING requires GROUP BY on SQLite
      def having(*cond)
        raise(InvalidOperation, "Can only specify a HAVING clause on a grouped dataset") unless @opts[:group]
        super
      end
      
      # SQLite uses the nonstandard ` (backtick) for quoting identifiers.
      def quoted_identifier_append(sql, c)
        sql << BACKTICK << c.to_s.gsub(BACKTICK_RE, DOUBLE_BACKTICK) << BACKTICK
      end
      
      # When a qualified column is selected on SQLite and the qualifier
      # is a subselect, the column name used is the full qualified name
      # (including the qualifier) instead of just the column name.  To
      # get correct column names, you must use an alias.
      def select(*cols)
        if ((f = @opts[:from]) && f.any?{|t| t.is_a?(Dataset) || (t.is_a?(SQL::AliasedExpression) && t.expression.is_a?(Dataset))}) || ((j = @opts[:join]) && j.any?{|t| t.table.is_a?(Dataset)})
          super(*cols.map{|c| alias_qualified_column(c)})
        else
          super
        end
      end
      
      # SQLite 3.8.3+ supports common table expressions.
      def supports_cte?(type=:select)
        db.sqlite_version >= 30803
      end

      # SQLite does not support table aliases with column aliases
      def supports_derived_column_lists?
        false
      end

      # SQLite does not support INTERSECT ALL or EXCEPT ALL
      def supports_intersect_except_all?
        false
      end

      # SQLite does not support IS TRUE
      def supports_is_true?
        false
      end
      
      # SQLite does not support multiple columns for the IN/NOT IN operators
      def supports_multiple_column_in?
        false
      end
      
      # SQLite supports timezones in literal timestamps, since it stores them
      # as text.  But using timezones in timestamps breaks SQLite datetime
      # functions, so we allow the user to override the default per database.
      def supports_timestamp_timezones?
        db.use_timestamp_timezones?
      end

      # SQLite cannot use WHERE 't'.
      def supports_where_true?
        false
      end
      
      private
      
      # SQLite uses string literals instead of identifiers in AS clauses.
      def as_sql_append(sql, aliaz, column_aliases=nil)
        raise Error, "sqlite does not support derived column lists" if column_aliases
        aliaz = aliaz.value if aliaz.is_a?(SQL::Identifier)
        sql << AS
        literal_append(sql, aliaz.to_s)
      end

      # If col is a qualified column, alias it to the same as the column name
      def alias_qualified_column(col)
        case col
        when Symbol
          t, c, a = split_symbol(col)
          if t && !a
            alias_qualified_column(SQL::QualifiedIdentifier.new(t, c))
          else
            col
          end
        when SQL::QualifiedIdentifier
          SQL::AliasedExpression.new(col, col.column)
        else
          col
        end
      end

      # SQLite supports a maximum of 500 rows in a VALUES clause.
      def default_import_slice
        500
      end

      # SQL fragment specifying a list of identifiers
      def identifier_list(columns)
        columns.map{|i| quote_identifier(i)}.join(COMMA)
      end
    
      # SQLite uses a preceding X for hex escaping strings
      def literal_blob_append(sql, v)
        sql << BLOB_START << v.unpack(HSTAR).first << APOS
      end

      # Respect the database integer_booleans setting, using 0 or 'f'.
      def literal_false
        @db.integer_booleans ? '0' : "'f'"
      end

      # Respect the database integer_booleans setting, using 1 or 't'.
      def literal_true
        @db.integer_booleans ? '1' : "'t'"
      end

      # SQLite only supporting multiple rows in the VALUES clause
      # starting in 3.7.11.  On older versions, fallback to using a UNION.
      def multi_insert_sql_strategy
        db.sqlite_version >= 30711 ? :values : :union
      end

      # SQLite does not support FOR UPDATE, but silently ignore it
      # instead of raising an error for compatibility with other
      # databases.
      def select_lock_sql(sql)
        super unless @opts[:lock] == :update
      end

      def select_only_offset_sql(sql)
        sql << ONLY_OFFSET
        literal_append(sql, @opts[:offset])
      end
  
      # SQLite supports quoted function names.
      def supports_quoted_function_names?
        true
      end

      # SQLite treats a DELETE with no WHERE clause as a TRUNCATE
      def _truncate_sql(table)
        "DELETE FROM #{table}"
      end
    end
  end
end
