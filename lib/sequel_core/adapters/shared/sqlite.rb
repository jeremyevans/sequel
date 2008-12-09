module Sequel
  module SQLite
    module DatabaseMethods
      AUTO_VACUUM = {'0' => :none, '1' => :full, '2' => :incremental}.freeze
      SYNCHRONOUS = {'0' => :off, '1' => :normal, '2' => :full}.freeze
      TABLES_FILTER = "type = 'table' AND NOT name = 'sqlite_sequence'"
      TEMP_STORE = {'0' => :default, '1' => :file, '2' => :memory}.freeze
      
      # Run all alter_table commands in a transaction.  This is technically only
      # needed for drop column.
      def alter_table(name, generator=nil, &block)
        transaction{super}
      end

      # SQLite supports limited table modification.  You can add a column
      # or an index.  Dropping columns is supported by copying the table into
      # a temporary table, dropping the table, and creating a new table without
      # the column inside of a transaction.
      def alter_table_sql(table, op)
        case op[:op]
        when :add_column
          super
        when :add_index
          index_definition_sql(table, op)
        when :drop_column
          columns_str = (schema_parse_table(table, {}).map{|c| c[0]} - Array(op[:name])).join(",")
          defined_columns_str = column_list_sql parse_pragma(table, {}).reject{ |c| c[:name] == op[:name].to_s}

          ["CREATE TEMPORARY TABLE #{table}_backup(#{defined_columns_str})",
           "INSERT INTO #{table}_backup SELECT #{columns_str} FROM #{table}",
           "DROP TABLE #{table}",
           "CREATE TABLE #{table}(#{defined_columns_str})",
           "INSERT INTO #{table} SELECT #{columns_str} FROM #{table}_backup",
           "DROP TABLE #{table}_backup"]
        else
          raise Error, "Unsupported ALTER TABLE operation"
        end
      end
      
      # A symbol signifying the value of the auto_vacuum PRAGMA.
      def auto_vacuum
        AUTO_VACUUM[pragma_get(:auto_vacuum).to_s]
      end
      
      # Set the auto_vacuum PRAGMA using the given symbol (:none, :full, or
      # :incremental).
      def auto_vacuum=(value)
        value = AUTO_VACUUM.key(value) || (raise Error, "Invalid value for auto_vacuum option. Please specify one of :none, :full, :incremental.")
        pragma_set(:auto_vacuum, value)
      end
      
      # Get the value of the given PRAGMA.
      def pragma_get(name)
        self["PRAGMA #{name}"].single_value
      end
      
      # Set the value of the given PRAGMA to value.
      def pragma_set(name, value)
        execute_ddl("PRAGMA #{name} = #{value}")
      end
      
      # A symbol signifying the value of the synchronous PRAGMA.
      def synchronous
        SYNCHRONOUS[pragma_get(:synchronous).to_s]
      end
      
      # Set the synchronous PRAGMA using the given symbol (:off, :normal, or :full).
      def synchronous=(value)
        value = SYNCHRONOUS.key(value) || (raise Error, "Invalid value for synchronous option. Please specify one of :off, :normal, :full.")
        pragma_set(:synchronous, value)
      end
      
      # Array of symbols specifying the table names in the current database.
      def tables
        self[:sqlite_master].filter(TABLES_FILTER).map {|r| r[:name].to_sym}
      end
      
      # A symbol signifying the value of the temp_store PRAGMA.
      def temp_store
        TEMP_STORE[pragma_get(:temp_store).to_s]
      end
      
      # Set the temp_store PRAGMA using the given symbol (:default, :file, or :memory).
      def temp_store=(value)
        value = TEMP_STORE.key(value) || (raise Error, "Invalid value for temp_store option. Please specify one of :default, :file, :memory.")
        pragma_set(:temp_store, value)
      end
      
      private
      
      # SQLite folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers by default.
      def upcase_identifiers_default
        false
      end

      # SQLite supports schema parsing using the table_info PRAGMA, so
      # parse the output of that into the format Sequel expects.
      def schema_parse_table(table_name, opts)
        parse_pragma(table_name, opts).map do |row|
          [row.delete(:name).to_sym, row]
        end
      end

      def parse_pragma(table_name, opts)
        self["PRAGMA table_info(?)", table_name].map do |row|
          row.delete(:cid)
          row[:allow_null] = row.delete(:notnull).to_i == 0
          row[:default] = row.delete(:dflt_value)
          row[:primary_key] = row.delete(:pk).to_i == 1
          row[:default] = nil if row[:default].blank?
          row[:db_type] = row.delete(:type)
          row[:type] = schema_column_type(row[:db_type])
          row
        end
      end
    end
    
    # Instance methods for datasets that connect to an SQLite database
    module DatasetMethods
      include Dataset::UnsupportedIntersectExceptAll

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
      
      # SQLite performs a TRUNCATE style DELETE if no filter is specified.
      # Since we want to always return the count of records, do a specific
      # count in the case of no filter.
      def delete(opts = {})
        # check if no filter is specified
        opts = @opts.merge(opts)
        unless opts[:where]
          @db.transaction(opts[:server]) do
            unfiltered_count = count
            execute_dui(delete_sql(opts))
            unfiltered_count
          end
        else
          execute_dui(delete_sql(opts))
        end
      end
      
      # Insert the values into the database.
      def insert(*values)
        execute_insert(insert_sql(*values))
      end
      
      # Allow inserting of values directly from a dataset.
      def insert_sql(*values)
        if (values.size == 1) && values.first.is_a?(Sequel::Dataset)
          "INSERT INTO #{source_list(@opts[:from])} #{values.first.sql};"
        else
          super(*values)
        end
      end
      
      # SQLite uses the nonstandard ` (backtick) for quoting identifiers.
      def quoted_identifier(c)
        "`#{c}`"
      end
      
      private
      
      # SQLite uses string literals instead of identifiers in AS clauses.
      def as_sql(expression, aliaz)
        "#{expression} AS #{literal(aliaz.to_s)}"
      end

      # Call execute_insert on the database with the given SQL.
      def execute_insert(sql, opts={})
        @db.execute_insert(sql, {:server=>@opts[:server] || :default}.merge(opts))
      end
    end
  end
end
