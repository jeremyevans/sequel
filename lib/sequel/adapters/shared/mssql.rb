module Sequel
  module MSSQL
    module DatabaseMethods
      AUTO_INCREMENT = 'IDENTITY(1,1)'.freeze
      SQL_BEGIN = "BEGIN TRANSACTION".freeze
      SQL_COMMIT = "COMMIT TRANSACTION".freeze
      SQL_ROLLBACK = "ROLLBACK TRANSACTION".freeze
      TEMPORARY = "#".freeze
      
      # Microsoft SQL Server uses the :mssql type.
      def database_type
        :mssql
      end

      def dataset(opts = nil)
        ds = super
        ds.extend(DatasetMethods)
        ds
      end
      
      # Microsoft SQL Server supports using the INFORMATION_SCHEMA to get
      # information on tables.
      def tables(opts={})
        m = output_identifier_meth
        metadata_dataset.from(:information_schema__tables___t).
          select(:table_name).
          filter(:table_type=>'BASE TABLE', :table_schema=>(opts[:schema]||default_schema||'dbo').to_s).
          map{|x| m.call(x[:table_name])}
      end

      private

      def auto_increment_sql
        AUTO_INCREMENT
      end
      
      def alter_table_sql(table, op)
        case op[:op]
        when :add_column
          "ALTER TABLE #{quote_schema_table(table)} ADD #{column_definition_sql(op)}"
        when :rename_column
          "SP_RENAME #{literal("#{quote_schema_table(table)}.#{quote_identifier(op[:name])}")}, #{literal(op[:new_name].to_s)}, 'COLUMN'"
        when :set_column_type
          "ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{quote_identifier(op[:name])} #{type_literal(op)}"
        when :set_column_null
          sch = schema(table).find{|k,v| k.to_s == op[:name].to_s}.last
          type = {:type=>sch[:db_type]}
          type[:size] = sch[:max_chars] if sch[:max_chars]
          "ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{quote_identifier(op[:name])} #{type_literal(type)} #{'NOT ' unless op[:null]}NULL"
        when :set_column_default
          "ALTER TABLE #{quote_schema_table(table)} ADD CONSTRAINT #{quote_identifier("sequel_#{table}_#{op[:name]}_def")} DEFAULT #{literal(op[:default])} FOR #{quote_identifier(op[:name])}"
        else
          super(table, op)
        end
      end

      # SQL to BEGIN a transaction.
      def begin_transaction_sql
        SQL_BEGIN
      end

      # SQL to COMMIT a transaction.
      def commit_transaction_sql
        SQL_COMMIT
      end
      
      # The SQL to drop an index for the table.
      def drop_index_sql(table, op)
        "DROP INDEX #{quote_identifier(op[:name] || default_index_name(table, op[:columns]))} ON #{quote_schema_table(table)}"
      end
      
      # SQL to ROLLBACK a transaction.
      def rollback_transaction_sql
        SQL_ROLLBACK
      end
      
      def schema_parse_table(table_name, opts)
        m = output_identifier_meth
        m2 = input_identifier_meth
        ds = metadata_dataset.from(:information_schema__tables___t).
         join(:information_schema__columns___c, :table_catalog=>:table_catalog,
              :table_schema => :table_schema, :table_name => :table_name).
         select(:column_name___column, :data_type___db_type, :character_maximum_length___max_chars, :column_default___default, :is_nullable___allow_null).
         filter(:c__table_name=>m2.call(table_name.to_s))
        if schema = opts[:schema] || default_schema
          ds.filter!(:table_schema=>schema)
        end
        ds.map do |row|
          row[:allow_null] = row[:allow_null] == 'YES' ? true : false
          row[:default] = nil if blank_object?(row[:default])
          row[:type] = schema_column_type(row[:db_type])
          [m.call(row.delete(:column)), row]
        end
      end

      # SQL fragment for marking a table as temporary
      def temporary_table_sql
        TEMPORARY
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
      
      # MSSQL uses image type for blobs
      def type_literal_generic_file(column)
        :image
      end
    end
  
    module DatasetMethods
      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      SELECT_CLAUSE_ORDER = %w'with limit distinct columns from table_options join where group order having compounds'.freeze
      TIMESTAMP_FORMAT = "'%Y-%m-%d %H:%M:%S'".freeze

      def complex_expression_sql(op, args)
        case op
        when :'||'
          super(:+, args)
        else
          super(op, args)
        end
      end
      
      def full_text_search(cols, terms, opts = {})
        filter("CONTAINS (#{literal(cols)}, #{literal(terms)})")
      end
      
      def multi_insert_sql(columns, values)
        values = values.map {|r| "SELECT #{expression_list(r)}" }.join(" UNION ALL ")
        ["#{insert_sql_base}#{source_list(@opts[:from])} (#{identifier_list(columns)}) #{values}"]
      end

      # Allows you to do .nolock on a query
      def nolock
        clone(:table_options => "(NOLOCK)")
      end

      def quoted_identifier(name)
        "[#{name}]"
      end

      # Microsoft SQL Server does not support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end
      
      def supports_is_true?
        false
      end
      
      # MSSQL 2005+ supports window functions
      def supports_window_functions?
        true
      end

      private
      
      # MSSQL uses a literal hexidecimal number for blob strings
      def literal_blob(v)
        blob = '0x'
        v.each_byte{|x| blob << sprintf('%02x', x)}
        blob
      end
      
      # Use unicode string syntax for all strings
      def literal_string(v)
        "N#{super}"
      end
      
      # Use MSSQL Timestamp format
      def literal_datetime(v)
        v.strftime(TIMESTAMP_FORMAT)
      end
      
      # Use 0 for false on MySQL
      def literal_false
        BOOL_FALSE
      end
      
      # Use MSSQL Timestamp format
      def literal_time(v)
        v.strftime(TIMESTAMP_FORMAT)
      end

      # Use 1 for true on MySQL
      def literal_true
        BOOL_TRUE
      end

      def select_clause_order
        SELECT_CLAUSE_ORDER
      end

      # MSSQL uses TOP for limit, with no offset support
      def select_limit_sql(sql)
        raise(Error, "OFFSET not supported") if @opts[:offset]
        sql << " TOP #{@opts[:limit]}" if @opts[:limit]
      end

      # MSSQL uses the WITH statement to lock tables
      def select_table_options_sql(sql)
        sql << " WITH #{@opts[:table_options]}" if @opts[:table_options]
      end
    end
  end
end
