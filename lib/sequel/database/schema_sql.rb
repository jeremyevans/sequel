module Sequel
  class Database
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
    TEMPORARY = 'TEMPORARY '.freeze
    TYPES = Hash.new {|h, k| k}
    TYPES.merge!(:double=>'double precision', String=>'varchar(255)',
      Integer=>'integer', Fixnum=>'integer', Bignum=>'bigint',
      Float=>'double precision', BigDecimal=>'numeric', Numeric=>'numeric',
      Date=>'date', DateTime=>'timestamp', Time=>'timestamp', File=>'blob',
      TrueClass=>'boolean', FalseClass=>'boolean')
    UNDERSCORE = '_'.freeze
    UNIQUE = ' UNIQUE'.freeze
    UNSIGNED = ' UNSIGNED'.freeze

    # Default serial primary key options.
    def serial_primary_key_options
      {:primary_key => true, :type => Integer, :auto_increment => true}
    end

    private

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
    # SQL for all given columns, used inside a CREATE TABLE block.
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

    # DDL statement for creating a table with the given name, columns, and options
    def create_table_sql(name, columns, options)
      "CREATE #{temporary_table_sql if options[:temp]}TABLE #{quote_schema_table(name)} (#{column_list_sql(columns)})"
    end

    # Array of SQL DDL statements, the first for creating a table with the given
    # name and column specifications, and the others for specifying indexes on
    # the table.
    def create_table_sql_list(name, columns, indexes, options = {})
      [create_table_sql(name, columns, options), (index_list_sql_list(name, indexes) unless indexes.empty?)].compact.flatten
    end
    
    # Default index name for the table and columns, may be too long
    # for certain databases.
    def default_index_name(table_name, columns)
      schema, table = schema_and_table(table_name)
      "#{"#{schema}_" if schema and schema != default_schema}#{table}_#{columns.map{|c| [String, Symbol].any?{|cl| c.is_a?(cl)} ? c : literal(c).gsub(/\W/, '_')}.join(UNDERSCORE)}_index"
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

    # SQL DDL fragment for temporary table
    def temporary_table_sql
      self.class.const_get(:TEMPORARY)
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
