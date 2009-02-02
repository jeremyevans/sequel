module Sequel
  module Schema
    module SQL
      AUTOINCREMENT = 'AUTOINCREMENT'.freeze
      CASCADE = 'CASCADE'.freeze
      COMMA_SEPARATOR = ', '.freeze
      NO_ACTION = 'NO ACTION'.freeze
      NOT_NULL = ' NOT NULL'.freeze
      NULL = ' NULL'.freeze
      PRIMARY_KEY = ' PRIMARY KEY'.freeze
      RESTRICT = 'RESTRICT'.freeze
      SET_DEFAULT = 'SET DEFAULT'.freeze
      SET_NULL = 'SET NULL'.freeze
      TYPES = Hash.new {|h, k| k}
      TYPES.merge!(:double=>'double precision', String=>'varchar',
        Integer=>'integer', Fixnum=>'integer', Bignum=>'bigint',
        Float=>'double precision', BigDecimal=>'numeric', Numeric=>'numeric',
        Date=>'date', DateTime=>'timestamp', Time=>'timestamp', File=>'blob',
        TrueClass=>'boolean', FalseClass=>'boolean')
      UNDERSCORE = '_'.freeze
      UNIQUE = ' UNIQUE'.freeze
      UNSIGNED = ' UNSIGNED'.freeze

      # The SQL to execute to modify the DDL for the given table name.  op
      # should be one of the operations returned by the AlterTableGenerator.
      def alter_table_sql(table, op)
        quoted_name = quote_identifier(op[:name]) if op[:name]
        alter_table_op = case op[:op]
        when :add_column
          "ADD COLUMN #{column_definition_sql(op)}"
        when :drop_column
          "DROP COLUMN #{quoted_name}"
        when :rename_column
          "RENAME COLUMN #{quoted_name} TO #{quote_identifier(op[:new_name])}"
        when :set_column_type
          "ALTER COLUMN #{quoted_name} TYPE #{type_literal(op)}"
        when :set_column_default
          "ALTER COLUMN #{quoted_name} SET DEFAULT #{literal(op[:default])}"
        when :set_column_null
          "ALTER COLUMN #{quoted_name} #{op[:null] ? 'DROP' : 'SET'} NOT NULL"
        when :add_index
          return index_definition_sql(table, op)
        when :drop_index
          return drop_index_sql(table, op)
        when :add_constraint
          "ADD #{constraint_definition_sql(op)}"
        when :drop_constraint
          "DROP CONSTRAINT #{quoted_name}"
        else
          raise Error, "Unsupported ALTER TABLE operation"
        end
        "ALTER TABLE #{quote_schema_table(table)} #{alter_table_op}"
      end

      # Array of SQL DDL modification statements for the given table,
      # corresponding to the DDL changes specified by the operations.
      def alter_table_sql_list(table, operations)
        operations.map{|op| alter_table_sql(table, op)}
      end
      
      # The SQL string specify the autoincrement property, generally used by
      # primary keys.
      def auto_increment_sql
        AUTOINCREMENT
      end
      
      # SQL DDL fragment containing the column creation SQL for the given column.
      def column_definition_sql(column)
        return constraint_definition_sql(column) if column[:type] == :check
        sql = "#{quote_identifier(column[:name])} #{type_literal(column)}"
        sql << UNIQUE if column[:unique]
        sql << NOT_NULL if column[:null] == false
        sql << NULL if column[:null] == true
        sql << " DEFAULT #{literal(column[:default])}" if column.include?(:default)
        sql << PRIMARY_KEY if column[:primary_key]
        sql << " #{auto_increment_sql}" if column[:auto_increment]
        sql << column_references_sql(column) if column[:table]
        sql
      end
      
      # SQL DDL fragment containing the column creation
      # SQL for all given columns, used instead a CREATE TABLE block.
      def column_list_sql(columns)
        columns.map{|c| column_definition_sql(c)}.join(COMMA_SEPARATOR)
      end

      # SQL DDL fragment for column foreign key references
      def column_references_sql(column)
        sql = " REFERENCES #{quote_schema_table(column[:table])}"
        sql << "(#{Array(column[:key]).map{|x| quote_identifier(x)}.join(COMMA_SEPARATOR)})" if column[:key]
        sql << " ON DELETE #{on_delete_clause(column[:on_delete])}" if column[:on_delete]
        sql << " ON UPDATE #{on_delete_clause(column[:on_update])}" if column[:on_update]
        sql
      end
    
      # SQL DDL fragment specifying a constraint on a table.
      def constraint_definition_sql(constraint)
        sql = constraint[:name] ? "CONSTRAINT #{quote_identifier(constraint[:name])} " : ""
        case constraint[:constraint_type]
        when :primary_key
          sql << "PRIMARY KEY #{literal(constraint[:columns])}"
        when :foreign_key
          sql << "FOREIGN KEY #{literal(constraint[:columns])}"
          sql << column_references_sql(constraint)
        when :unique
          sql << "UNIQUE #{literal(constraint[:columns])}"
        else
          check = constraint[:check]
          sql << "CHECK #{filter_expr((check.is_a?(Array) && check.length == 1) ? check.first : check)}"
        end
        sql
      end

      # Array of SQL DDL statements, the first for creating a table with the given
      # name and column specifications, and the others for specifying indexes on
      # the table.
      def create_table_sql_list(name, columns, indexes = nil, options = {})
        sql = ["CREATE TABLE #{quote_schema_table(name)} (#{column_list_sql(columns)})"]
        sql.concat(index_list_sql_list(name, indexes)) if indexes && !indexes.empty?
        sql
      end
      
      # Default index name for the table and columns, may be too long
      # for certain databases.
      def default_index_name(table_name, columns)
        schema, table = schema_and_table(table_name)
        "#{"#{schema}_" if schema and schema != default_schema}#{table}_#{columns.join(UNDERSCORE)}_index"
      end
    
      # The SQL to drop an index for the table.
      def drop_index_sql(table, op)
        "DROP INDEX #{quote_identifier(op[:name] || default_index_name(table, op[:columns]))}"
      end

      # SQL DDL statement to drop the table with the given name.
      def drop_table_sql(name)
        "DROP TABLE #{quote_schema_table(name)}"
      end
      
      # Proxy the filter_expr call to the dataset, used for creating constraints.
      def filter_expr(*args, &block)
        schema_utility_dataset.literal(schema_utility_dataset.send(:filter_expr, *args, &block))
      end

      # SQL DDL statement for creating an index for the table with the given name
      # and index specifications.
      def index_definition_sql(table_name, index)
        index_name = index[:name] || default_index_name(table_name, index[:columns])
        if index[:type]
          raise Error, "Index types are not supported for this database"
        elsif index[:where]
          raise Error, "Partial indexes are not supported for this database"
        else
          "CREATE #{'UNIQUE ' if index[:unique]}INDEX #{quote_identifier(index_name)} ON #{quote_identifier(table_name)} #{literal(index[:columns])}"
        end
      end
    
      # Array of SQL DDL statements, one for each index specification,
      # for the given table.
      def index_list_sql_list(table_name, indexes)
        indexes.map{|i| index_definition_sql(table_name, i)}
      end
  
      # Proxy the literal call to the dataset, used for default values.
      def literal(v)
        schema_utility_dataset.literal(v)
      end
      
      # SQL DDL ON DELETE fragment to use, based on the given action.
      # The following actions are recognized:
      # 
      # * :cascade - Delete rows referencing this row.
      # * :no_action (default) - Raise an error if other rows reference this
      #   row, allow deferring of the integrity check.
      # * :restrict - Raise an error if other rows reference this row,
      #   but do not allow deferring the integrity check.
      # * :set_default - Set columns referencing this row to their default value.
      # * :set_null - Set columns referencing this row to NULL.
      def on_delete_clause(action)
        case action
        when :restrict
          RESTRICT
        when :cascade
          CASCADE
        when :set_null
          SET_NULL
        when :set_default
          SET_DEFAULT
        else
          NO_ACTION
        end
      end
      
      # Proxy the quote_schema_table method to the dataset
      def quote_schema_table(table)
        schema_utility_dataset.quote_schema_table(table)
      end
      
      # Proxy the quote_identifier method to the dataset, used for quoting tables and columns.
      def quote_identifier(v)
        schema_utility_dataset.quote_identifier(v)
      end
      
      # SQL DDL statement for renaming a table.
      def rename_table_sql(name, new_name)
        "ALTER TABLE #{quote_schema_table(name)} RENAME TO #{quote_schema_table(new_name)}"
      end

      # Parse the schema from the database.
      # If the table_name is not given, returns the schema for all tables as a hash.
      # If the table_name is given, returns the schema for a single table as an
      # array with all members being arrays of length 2.  Available options are:
      #
      # * :reload - Get fresh information from the database, instead of using
      #   cached information.  If table_name is blank, :reload should be used
      #   unless you are sure that schema has not been called before with a
      #   table_name, otherwise you may only getting the schemas for tables
      #   that have been requested explicitly.
      # * :schema - An explicit schema to use.  It may also be implicitly provided
      #   via the table name.
      def schema(table = nil, opts={})
        raise(Error, 'schema parsing is not implemented on this database') unless respond_to?(:schema_parse_table, true)

        if table
          sch, table_name = schema_and_table(table)
          quoted_name = quote_schema_table(table)
        end
        opts = opts.merge(:schema=>sch) if sch && !opts.include?(:schema)
        if opts[:reload] && @schemas
          if table_name
            @schemas.delete(quoted_name)
          else
            @schemas = nil
          end
        end

        if @schemas
          if table_name
            return @schemas[quoted_name] if @schemas[quoted_name]
          else
            return @schemas
          end
        end
        
        raise(Error, '#tables does not exist, you must provide a specific table to #schema') if table.nil? && !respond_to?(:tables, true)

        @schemas ||= Hash.new do |h,k|
          quote_name = quote_schema_table(k)
          h[quote_name] if h.include?(quote_name)
        end

        if table_name
          cols = schema_parse_table(table_name, opts)
          raise(Error, 'schema parsing returned no columns, table probably doesn\'t exist') if cols.blank?
          @schemas[quoted_name] = cols
        else
          tables.each{|t| @schemas[quote_schema_table(t)] = schema_parse_table(t.to_s, opts)}
          @schemas
        end
      end
      
      # The dataset to use for proxying certain schema methods.
      def schema_utility_dataset
        @schema_utility_dataset ||= dataset
      end
      
      private

      # Remove the cached schema for the given schema name
      def remove_cached_schema(table)
        @schemas.delete(quote_schema_table(table)) if @schemas
      end
      
      # Remove the cached schema_utility_dataset, because the identifier
      # quoting has changed.
      def reset_schema_utility_dataset
        @schema_utility_dataset = nil
      end

      # Match the database's column type to a ruby type via a
      # regular expression.  The following ruby types are supported:
      # integer, string, date, datetime, boolean, and float.
      def schema_column_type(db_type)
        case db_type
        when /\Atinyint/io
          Sequel.convert_tinyint_to_bool ? :boolean : :integer
        when /\Ainterval\z/io
          :interval
        when /\A(character( varying)?|varchar|text)/io
          :string
        when /\A(int(eger)?|bigint|smallint)/io
          :integer
        when /\Adate\z/io
          :date
        when /\A(datetime|timestamp( with(out)? time zone)?)\z/io
          :datetime
        when /\Atime( with(out)? time zone)?\z/io
          :time
        when /\Aboolean\z/io
          :boolean
        when /\A(real|float|double( precision)?)\z/io
          :float
        when /\A(numeric(\(\d+,\d+\))?|decimal|money)\z/io
          :decimal
        when /\Abytea\z/io
          :blob
        end
      end

      # SQL fragment specifying the type of a given column.
      def type_literal(column)
        type = type_literal_base(column)
        column[:size] ||= 255 if type.to_s == 'varchar'
        elements = column[:size] || column[:elements]
        "#{type}#{literal(Array(elements)) if elements}#{UNSIGNED if column[:unsigned]}"
      end

      # SQL fragment specifying the base type of a given column,
      # without the size or elements.
      def type_literal_base(column)
        TYPES[column[:type]]
      end
    end
  end
end
