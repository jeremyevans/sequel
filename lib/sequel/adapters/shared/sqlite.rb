module Sequel
  module SQLite
    module DatabaseMethods
      AUTO_VACUUM = [:none, :full, :incremental].freeze
      PRIMARY_KEY_INDEX_RE = /\Asqlite_autoindex_/.freeze
      SYNCHRONOUS = [:off, :normal, :full].freeze
      TABLES_FILTER = "type = 'table' AND NOT name = 'sqlite_sequence'"
      TEMP_STORE = [:default, :file, :memory].freeze
      
      # Run all alter_table commands in a transaction.  This is technically only
      # needed for drop column.
      def alter_table(name, generator=nil, &block)
        remove_cached_schema(name)
        generator ||= Schema::AlterTableGenerator.new(self, &block)
        transaction{generator.operations.each{|op| alter_table_sql_list(name, [op]).flatten.each{|sql| execute_ddl(sql)}}}
      end

      # A symbol signifying the value of the auto_vacuum PRAGMA.
      def auto_vacuum
        AUTO_VACUUM[pragma_get(:auto_vacuum).to_i]
      end
      
      # Set the auto_vacuum PRAGMA using the given symbol (:none, :full, or
      # :incremental).
      def auto_vacuum=(value)
        value = AUTO_VACUUM.index(value) || (raise Error, "Invalid value for auto_vacuum option. Please specify one of :none, :full, :incremental.")
        pragma_set(:auto_vacuum, value)
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
      # SQLite 3.6.19+.  If not using 3.6.19+, no error is raised.
      def foreign_keys=(value)
        pragma_set(:foreign_keys, !!value ? 'on' : 'off') if sqlite_version >= 30619
      end

      # Return a hash containing index information. Hash keys are index name symbols.
      # Values are subhashes with two keys, :columns and :unique.  The value of :columns
      # is an array of symbols of column names.  The value of :unique is true or false
      # depending on if the index is unique.
      def indexes(table)
        m = output_identifier_meth
        im = input_identifier_meth
        indexes = {}
        begin
          metadata_dataset.with_sql("PRAGMA index_list(?)", im.call(table)).each do |r|
            next if r[:name] =~ PRIMARY_KEY_INDEX_RE
            indexes[m.call(r[:name])] = {:unique=>r[:unique].to_i==1}
          end
        rescue Sequel::DatabaseError
          nil
        else
          indexes.each do |k, v|
            v[:columns] = metadata_dataset.with_sql("PRAGMA index_info(?)", im.call(k)).map(:name).map{|x| m.call(x)}
          end
        end
        indexes
      end

      # Get the value of the given PRAGMA.
      def pragma_get(name)
        self["PRAGMA #{name}"].single_value
      end
      
      # Set the value of the given PRAGMA to value.
      def pragma_set(name, value)
        execute_ddl("PRAGMA #{name} = #{value}")
      end
      
      # The version of the server as an integer, where 3.6.19 = 30619.
      # If the server version can't be determined, 0 is used.
      def sqlite_version
        return @server_version if defined?(@server_version)
        @server_version = begin
          v = get{sqlite_version{}}
          [10000, 100, 1].zip(v.split('.')).inject(0){|a, m| a + m[0] * Integer(m[1])}
        rescue
          0
        end
      end
      
      # SQLite 3.6.8+ supports savepoints. 
      def supports_savepoints?
        sqlite_version >= 30608
      end

      # A symbol signifying the value of the synchronous PRAGMA.
      def synchronous
        SYNCHRONOUS[pragma_get(:synchronous).to_i]
      end
      
      # Set the synchronous PRAGMA using the given symbol (:off, :normal, or :full).
      def synchronous=(value)
        value = SYNCHRONOUS.index(value) || (raise Error, "Invalid value for synchronous option. Please specify one of :off, :normal, :full.")
        pragma_set(:synchronous, value)
      end
      
      # Array of symbols specifying the table names in the current database.
      #
      # Options:
      # * :server - Set the server to use.
      def tables(opts={})
        m = output_identifier_meth
        metadata_dataset.from(:sqlite_master).server(opts[:server]).filter(TABLES_FILTER).map{|r| m.call(r[:name])}
      end
      
      # A symbol signifying the value of the temp_store PRAGMA.
      def temp_store
        TEMP_STORE[pragma_get(:temp_store).to_i]
      end
      
      # Set the temp_store PRAGMA using the given symbol (:default, :file, or :memory).
      def temp_store=(value)
        value = TEMP_STORE.index(value) || (raise Error, "Invalid value for temp_store option. Please specify one of :default, :file, :memory.")
        pragma_set(:temp_store, value)
      end
      
      private

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
          duplicate_table(table){|columns| columns.each{|s| s[:type] = op[:type] if s[:name].to_s == op[:name].to_s}}
        else
          raise Error, "Unsupported ALTER TABLE operation"
        end
      end
      
      # The array of column symbols in the table, except for ones given in opts[:except]
      def backup_table_name(table, opts={})
        table = table.gsub('`', '')
        (opts[:times]||1000).times do |i|
          table_name = "#{table}_backup#{i}"
          return table_name unless table_exists?(table_name)
        end
      end

      # Allow use without a generator, needed for the alter table hackery that Sequel allows.
      def column_list_sql(generator)
        generator.is_a?(Schema::Generator) ? super : generator.map{|c| column_definition_sql(c)}.join(', ')
      end

      # The array of column schema hashes, except for the ones given in opts[:except]
      def defined_columns_for(table, opts={})
        cols = parse_pragma(table, {})
        cols.each do |c|
          c[:default] = LiteralString.new(c[:default]) if c[:default]
          c[:type] = c[:db_type]
        end
        if opts[:except]
          nono= Array(opts[:except]).compact.map{|n| n.to_s}
          cols.reject!{|c| nono.include? c[:name] }
        end
        cols
      end
      
      # Duplicate an existing table by creating a new table, copying all records
      # from the existing table into the new table, deleting the existing table
      # and renaming the new table to the existing table's name.
      def duplicate_table(table, opts={})
        remove_cached_schema(table)
        def_columns = defined_columns_for(table)
        old_columns = def_columns.map{|c| c[:name]}
        opts[:old_columns_proc].call(old_columns) if opts[:old_columns_proc]

        yield def_columns if block_given?
        def_columns_str = column_list_sql(def_columns)
        new_columns = old_columns.dup
        opts[:new_columns_proc].call(new_columns) if opts[:new_columns_proc]

        qt = quote_schema_table(table)
        bt = quote_identifier(backup_table_name(qt))
        [
           "CREATE TABLE #{bt}(#{def_columns_str})",
           "INSERT INTO #{bt}(#{dataset.send(:identifier_list, new_columns)}) SELECT #{dataset.send(:identifier_list, old_columns)} FROM #{qt}",
           "DROP TABLE #{qt}",
           "ALTER TABLE #{bt} RENAME TO #{qt}"
        ]
      end

      # SQLite folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on input.
      def identifier_input_method_default
        nil
      end
      
      # SQLite folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on output.
      def identifier_output_method_default
        nil
      end

      # Parse the output of the table_info pragma
      def parse_pragma(table_name, opts)
        metadata_dataset.with_sql("PRAGMA table_info(?)", input_identifier_meth.call(table_name)).map do |row|
          row.delete(:cid)
          row[:allow_null] = row.delete(:notnull).to_i == 0
          row[:default] = row.delete(:dflt_value)
          row[:primary_key] = row.delete(:pk).to_i == 1
          row[:default] = nil if blank_object?(row[:default]) || row[:default] == 'NULL'
          row[:db_type] = row.delete(:type)
          row[:type] = schema_column_type(row[:db_type])
          row
        end
      end
      
      # SQLite treats integer primary keys as autoincrementing (alias of rowid).
      def schema_autoincrementing_primary_key?(schema)
        super and schema[:db_type].downcase == 'integer'
      end

      # SQLite supports schema parsing using the table_info PRAGMA, so
      # parse the output of that into the format Sequel expects.
      def schema_parse_table(table_name, opts)
        m = output_identifier_meth
        parse_pragma(table_name, opts).map do |row|
          [m.call(row.delete(:name)), row]
        end
      end
      
      # SQLite uses the integer data type even for bignums.  This is because they
      # are both stored internally as text, and converted when returned from
      # the database.  Using an integer type instead of bigint makes it more likely
      # that software will automatically return the column as an integer.
      def type_literal_generic_bignum(column)
        :integer
      end
    end
    
    # Instance methods for datasets that connect to an SQLite database
    module DatasetMethods
      SELECT_CLAUSE_METHODS = Dataset.clause_methods(:select, %w'distinct columns from join where group having compounds order limit')
      CONSTANT_MAP = {:CURRENT_DATE=>"date(CURRENT_TIMESTAMP, 'localtime')".freeze, :CURRENT_TIMESTAMP=>"datetime(CURRENT_TIMESTAMP, 'localtime')".freeze, :CURRENT_TIME=>"time(CURRENT_TIMESTAMP, 'localtime')".freeze}
    
      # SQLite does not support pattern matching via regular expressions.
      # SQLite is case insensitive (depending on pragma), so use LIKE for
      # ILIKE.
      def complex_expression_sql(op, args)
        case op
        when :~, :'!~', :'~*', :'!~*'
          raise Error, "SQLite does not support pattern matching via regular expressions"
        when :LIKE, :'NOT LIKE', :ILIKE, :'NOT ILIKE'
          # SQLite is case insensitive for ASCII, and non case sensitive for other character sets
          "#{'NOT ' if [:'NOT LIKE', :'NOT ILIKE'].include?(op)}(#{literal(args.at(0))} LIKE #{literal(args.at(1))})"
        else
          super(op, args)
        end
      end
      
      # MSSQL doesn't support the SQL standard CURRENT_DATE or CURRENT_TIME
      def constant_sql(constant)
        CONSTANT_MAP[constant] || super
      end
      
      # SQLite performs a TRUNCATE style DELETE if no filter is specified.
      # Since we want to always return the count of records, add a condition
      # that is always true and then delete.
      def delete
        @opts[:where] ? super : filter(1=>1).delete
      end
      
      # Return an array of strings specifying a query explanation for a SELECT of the
      # current dataset.
      def explain
        db.send(:metadata_dataset).clone(:sql=>"EXPLAIN #{select_sql}").
          map{|x| "#{x[:addr]}|#{x[:opcode]}|#{(1..5).map{|i| x[:"p#{i}"]}.join('|')}|#{x[:comment]}"}
      end
      
      # HAVING requires GROUP BY on SQLite
      def having(*cond, &block)
        raise(InvalidOperation, "Can only specify a HAVING clause on a grouped dataset") unless @opts[:group]
        super
      end
      
      # SQLite uses the nonstandard ` (backtick) for quoting identifiers.
      def quoted_identifier(c)
        "`#{c}`"
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
      # as text.
      def supports_timestamp_timezones?
        true
      end

      private
      
      # SQLite uses string literals instead of identifiers in AS clauses.
      def as_sql(expression, aliaz)
        aliaz = aliaz.value if aliaz.is_a?(SQL::Identifier)
        "#{expression} AS #{literal(aliaz.to_s)}"
      end
      
      # SQLite uses a preceding X for hex escaping strings
      def literal_blob(v)
        blob = ''
        v.each_byte{|x| blob << sprintf('%02x', x)}
        "X'#{blob}'"
      end
      
      # SQLite does not support the SQL WITH clause
      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end
      
      # Support FOR SHARE locking when using the :share lock style.
      def select_lock_sql(sql)
        super unless @opts[:lock] == :update
      end
      
      # SQLite treats a DELETE with no WHERE clause as a TRUNCATE
      def _truncate_sql(table)
        "DELETE FROM #{table}"
      end
    end
  end
end
