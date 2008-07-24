module Sequel
  module SQLite
    module DatabaseMethods
      AUTO_VACUUM = {'0' => :none, '1' => :full, '2' => :incremental}.freeze
      SCHEMA_TYPE_RE = /\A(\w+)\((\d+)\)\z/
      SYNCHRONOUS = {'0' => :off, '1' => :normal, '2' => :full}.freeze
      TABLES_FILTER = "type = 'table' AND NOT name = 'sqlite_sequence'"
      TEMP_STORE = {'0' => :default, '1' => :file, '2' => :memory}.freeze
      
      def alter_table_sql(table, op)
        case op[:op]
        when :add_column
          super
        when :add_index
          index_definition_sql(table, op)
        when :drop_column
          columns_str = (schema_parse_table(table, {}).map{|c| c[0]} - Array(op[:name])).join(",")
          ["BEGIN TRANSACTION",
           "CREATE TEMPORARY TABLE #{table}_backup(#{columns_str})",
           "INSERT INTO #{table}_backup SELECT #{columns_str} FROM #{table}",
           "DROP TABLE #{table}",
           "CREATE TABLE #{table}(#{columns_str})",
           "INSERT INTO #{table} SELECT #{columns_str} FROM #{table}_backup",
           "DROP TABLE #{table}_backup",
           "COMMIT"]
        else
          raise Error, "Unsupported ALTER TABLE operation"
        end
      end
      
      def auto_vacuum
        AUTO_VACUUM[pragma_get(:auto_vacuum).to_s]
      end
      
      def auto_vacuum=(value)
        value = AUTO_VACUUM.key(value) || (raise Error, "Invalid value for auto_vacuum option. Please specify one of :none, :full, :incremental.")
        pragma_set(:auto_vacuum, value)
      end
      
      def pragma_get(name)
        self["PRAGMA #{name}"].single_value
      end
      
      def pragma_set(name, value)
        execute_ddl("PRAGMA #{name} = #{value}")
      end
      
      def serial_primary_key_options
        {:primary_key => true, :type => :integer, :auto_increment => true}
      end
      
      def synchronous
        SYNCHRONOUS[pragma_get(:synchronous).to_s]
      end
      
      def synchronous=(value)
        value = SYNCHRONOUS.key(value) || (raise Error, "Invalid value for synchronous option. Please specify one of :off, :normal, :full.")
        pragma_set(:synchronous, value)
      end
    
      def tables
        self[:sqlite_master].filter(TABLES_FILTER).map {|r| r[:name].to_sym}
      end
      
      def temp_store
        TEMP_STORE[pragma_get(:temp_store).to_s]
      end
      
      def temp_store=(value)
        value = TEMP_STORE.key(value) || (raise Error, "Invalid value for temp_store option. Please specify one of :default, :file, :memory.")
        pragma_set(:temp_store, value)
      end
      
      private

      def schema_parse_table(table_name, opts)
        rows = self["PRAGMA table_info(?)", table_name].collect do |row|
          row.delete(:cid)
          row[:column] = row.delete(:name)
          row[:allow_null] = row.delete(:notnull).to_i == 0 ? 'YES' : 'NO'
          row[:default] = row.delete(:dflt_value)
          row[:primary_key] = row.delete(:pk).to_i == 1 ? true : false 
          row[:db_type] = row.delete(:type)
          if m = SCHEMA_TYPE_RE.match(row[:db_type])
            row[:db_type] = m[1]
            row[:max_chars] = m[2].to_i
          else
             row[:max_chars] = nil
          end
          row[:numeric_precision] = nil
          row
        end
        schema_parse_rows(rows)
      end

      def schema_parse_tables(opts)
        schemas = {}
        tables.each{|table| schemas[table] = schema_parse_table(table, opts)}
        schemas
      end
    end
  
    module DatasetMethods      
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
      
      def delete(opts = nil)
        # check if no filter is specified
        unless (opts && opts[:where]) || @opts[:where]
          @db.transaction do
            unfiltered_count = count
            @db.execute_dui delete_sql(opts)
            unfiltered_count
          end
        else
          @db.execute_dui delete_sql(opts)
        end
      end
      
      def insert(*values)
        @db.execute_insert insert_sql(*values)
      end

      def insert_sql(*values)
        if (values.size == 1) && values.first.is_a?(Sequel::Dataset)
          "INSERT INTO #{source_list(@opts[:from])} #{values.first.sql};"
        else
          super(*values)
        end
      end
      
      def quoted_identifier(c)
        "`#{c}`"
      end
    end
  end
end
