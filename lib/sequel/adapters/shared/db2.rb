Sequel.require 'adapters/utils/emulate_offset_with_row_number'

module Sequel
  module DB2
    @use_clob_as_blob = true

    class << self
      # Whether to use clob as the generic File type, true by default.
      attr_accessor :use_clob_as_blob
    end

    module DatabaseMethods
      AUTOINCREMENT = 'GENERATED ALWAYS AS IDENTITY'.freeze
      NOT_NULL      = ' NOT NULL'.freeze
      NULL          = ''.freeze

      # DB2 always uses :db2 as it's database type
      def database_type
        :db2
      end
      
      # Return the database version as a string.  Don't rely on this,
      # it may return an integer in the future.
      def db2_version
        return @db2_version if @db2_version
        @db2_version = metadata_dataset.with_sql("select service_level from sysibmadm.env_inst_info").first[:service_level]
      end
      alias_method :server_version, :db2_version

      # Use SYSIBM.SYSCOLUMNS to get the information on the tables.
      def schema_parse_table(table, opts = {})
        m = output_identifier_meth(opts[:dataset])
        im = input_identifier_meth(opts[:dataset])
        metadata_dataset.with_sql("SELECT * FROM SYSIBM.SYSCOLUMNS WHERE TBNAME = #{literal(im.call(table))} ORDER BY COLNO").
          collect do |column| 
            column[:db_type]     = column.delete(:typename)
            if column[:db_type]  == "DECIMAL"
              column[:db_type] << "(#{column[:longlength]},#{column[:scale]})"
            end
            column[:allow_null]  = column.delete(:nulls) == 'Y'
            column[:primary_key] = column.delete(:identity) == 'Y' || !column[:keyseq].nil?
            column[:type]        = schema_column_type(column[:db_type])
            [ m.call(column.delete(:name)), column]
          end
      end

      # Use SYSCAT.TABLES to get the tables for the database
      def tables
        metadata_dataset.
          with_sql("SELECT TABNAME FROM SYSCAT.TABLES WHERE TYPE='T' AND OWNER = #{literal(input_identifier_meth.call(opts[:user]))}").
          all.map{|h| output_identifier_meth.call(h[:tabname]) }
      end

      # Use SYSCAT.TABLES to get the views for the database
      def views
        metadata_dataset.
          with_sql("SELECT TABNAME FROM SYSCAT.TABLES WHERE TYPE='V' AND OWNER = #{literal(input_identifier_meth.call(opts[:user]))}").
          all.map{|h| output_identifier_meth.call(h[:tabname]) }
      end

      # Use SYSCAT.INDEXES to get the indexes for the table
      def indexes(table, opts = {})
        metadata_dataset.
          with_sql("SELECT INDNAME,UNIQUERULE,MADE_UNIQUE,SYSTEM_REQUIRED FROM SYSCAT.INDEXES WHERE TABNAME = #{literal(input_identifier_meth.call(table))}").
          all.map{|h| Hash[ h.map{|k,v| [k.to_sym, v]} ] }
      end

      private

      # Handle DB2 specific alter table operations.
      def alter_table_sql(table, op)
        case op[:op]
        when :add_column
          if op[:primary_key] && op[:auto_increment] && op[:type] == Integer
            [
            "ALTER TABLE #{quote_schema_table(table)} ADD #{column_definition_sql(op.merge(:auto_increment=>false, :primary_key=>false, :default=>0, :null=>false))}",
            "ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{literal(op[:name])} DROP DEFAULT",
            "ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{literal(op[:name])} SET #{AUTOINCREMENT}"
            ]
          else
            "ALTER TABLE #{quote_schema_table(table)} ADD #{column_definition_sql(op)}"
          end
        when :drop_column
          "ALTER TABLE #{quote_schema_table(table)} DROP #{column_definition_sql(op)}"
        when :rename_column       # renaming is only possible after db2 v9.7
          "ALTER TABLE #{quote_schema_table(table)} RENAME COLUMN #{quote_identifier(op[:name])} TO #{quote_identifier(op[:new_name])}"
        when :set_column_type
          "ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{quote_identifier(op[:name])} SET DATA TYPE #{type_literal(op)}"
        when :set_column_default
          "ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{quote_identifier(op[:name])} SET DEFAULT #{literal(op[:default])}"
        when :add_constraint
          if op[:type] == :unique
            sqls = op[:columns].map{|c| ["ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{quote_identifier(c)} SET NOT NULL", reorg_sql(table)]}
            sqls << super
            sqls.flatten
          else
            super
          end
        else
          super
        end
      end

      # DB2 uses an identity column for autoincrement.
      def auto_increment_sql
        AUTOINCREMENT
      end

      # Add null/not null SQL fragment to column creation SQL.
      def column_definition_null_sql(sql, column)
        null = column.fetch(:null, column[:allow_null]) 
        null = false  if column[:primary_key]

        sql << NOT_NULL if null == false
        sql << NULL if null == true
      end

      # Supply columns with NOT NULL if they are part of a composite
      # primary/foreign key
      def column_list_sql(g)
        ks = []
        g.constraints.each{|c| ks = c[:columns] if [:primary_key, :foreign_key].include? c[:type]} 
        g.columns.each{|c| c[:null] = false if ks.include?(c[:name]) }
        super
      end

      # Here we use DGTT which has most backward compatibility, which uses
      # DECLARE instead of CREATE. CGTT can only be used after version 9.7.
      # http://www.ibm.com/developerworks/data/library/techarticle/dm-0912globaltemptable/
      def create_table_sql(name, generator, options)
        if options[:temp]
          "DECLARE GLOBAL TEMPORARY TABLE #{quote_identifier(name)} (#{column_list_sql(generator)})"
        else
          super
        end
      end

      # DB2 has issues with quoted identifiers, so
      # turn off database quoting by default.
      def quote_identifiers_default
        false
      end

      # DB2 uses RENAME TABLE to rename tables.
      def rename_table_sql(name, new_name)
        "RENAME TABLE #{quote_schema_table(name)} TO #{quote_schema_table(new_name)}"
      end

      # Run the REORG TABLE command for the table, necessary when
      # the table has been altered.
      def reorg(table)
        synchronize(opts[:server]){|c| c.execute(reorg_sql(table))}
      end

      # The SQL to use for REORGing a table.
      def reorg_sql(table)
        "CALL ADMIN_CMD(#{literal("REORG TABLE #{table}")})"
      end

      # We uses the clob type by default for Files.
      # Note: if user select to use blob, then insert statement should use 
      # use this for blob value:
      #     cast(X'fffefdfcfbfa' as blob(2G))
      def type_literal_generic_file(column)
        ::Sequel::DB2::use_clob_as_blob ? :clob : :blob
      end

      # DB2 uses smallint to store booleans.
      def type_literal_generic_trueclass(column)
        :smallint
      end
      alias type_literal_generic_falseclass type_literal_generic_trueclass
    end

    module DatasetMethods
      include EmulateOffsetWithRowNumber

      BITWISE_METHOD_MAP = {:& =>:BITAND, :| => :BITOR, :^ => :BITXOR, :'B~'=>:BITNOT}
      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      
      # DB2 casts strings using RTRIM and CHAR instead of VARCHAR.
      def cast_sql(expr, type)
        type == String ?  "RTRIM(CHAR(#{literal(expr)}))" : super
      end

      # Handle DB2 specific LIKE and bitwise operator support, and
      # emulate the extract method, which DB2 doesn't natively support.
      def complex_expression_sql(op, args)
        case op
        when :ILIKE
          super(:LIKE, [SQL::Function.new(:upper, args.at(0)), SQL::Function.new(:upper, args.at(1)) ])
        when :"NOT ILIKE"
          super(:"NOT LIKE", [SQL::Function.new(:upper, args.at(0)), SQL::Function.new(:upper, args.at(1)) ])
        when :&, :|, :^
          # works with db2 v9.5 and after
          op = BITWISE_METHOD_MAP[op]
          complex_expression_arg_pairs(args){|a, b| literal(SQL::Function.new(op, a, b))}
        when :<<
          complex_expression_arg_pairs(args){|a, b| "(#{literal(a)} * POWER(2, #{literal(b)}))"}
        when :>>
          complex_expression_arg_pairs(args){|a, b| "(#{literal(a)} / POWER(2, #{literal(b)}))"}
        when :'B~'
          literal(SQL::Function.new(:BITNOT, *args))
        when :extract
          "#{args.at(0)}(#{literal(args.at(1))})"
        else
          super
        end
      end

      # DB2 does not support IS TRUE.
      def supports_is_true?
        false
      end

      # DB2 does not support multiple columns in IN.
      def supports_multiple_column_in?
        false
      end

      # DB2 only allows * in SELECT if it is the only thing being selected.
      def supports_select_all_and_column?
        false
      end

      # DB2 does not support fractional seconds in timestamps.
      def supports_timestamp_usecs?
        false
      end

      # DB2 supports window functions
      def supports_window_functions?
        true
      end

      # DB2 does not support WHERE 1.
      def supports_where_true?
        false
      end

      private

      # DB2 needs the standard workaround to insert all default values into
      # a table with more than one column.
      def insert_supports_empty_values?
        false
      end

      # Use 0 for false on DB2
      def literal_false
        BOOL_FALSE
      end

      # Use 1 for true on DB2
      def literal_true
        BOOL_TRUE
      end

      # Add a fallback table for empty from situation
      def select_from_sql(sql)
        @opts[:from] ? super : (sql << ' FROM "SYSIBM"."SYSDUMMY1"')
      end

      # Modify the sql to limit the number of rows returned
      # Note: 
      #
      #     After db2 v9.7, MySQL flavored "LIMIT X OFFSET Y" can be enabled using
      #
      #     db2set DB2_COMPATIBILITY_VECTOR=MYSQL
      #     db2stop
      #     db2start
      #
      #     Support for this feature is not used in this adapter however.
      def select_limit_sql(sql)
        if l = @opts[:limit]
          sql << " FETCH FIRST #{l == 1 ? 'ROW' : "#{literal(l)} ROWS"} ONLY"
        end
      end
      
      def _truncate_sql(table)
        # "TRUNCATE #{table} IMMEDIATE" is only for newer version of db2, so we
        # use the following one
        "ALTER TABLE #{quote_schema_table(table)} ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
      end
    end
  end
end
