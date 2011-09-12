module Sequel
  module DB2
    module DatabaseMethods
      AUTOINCREMENT = 'GENERATED ALWAYS AS IDENTITY'.freeze
      NOT_NULL      = ' NOT NULL'.freeze
      NULL          = ''.freeze

      def database_type
        :db2
      end
      
      def db2_version
        return @db2_version if @db2_version
        @db2_version = metadata_dataset.with_sql("select service_level from sysibmadm.env_inst_info").first[:service_level]
      end
      alias_method :server_version, :db2_version

      def schema_parse_table(table, opts = {})
        m = output_identifier_meth
        im = input_identifier_meth
        metadata_dataset.with_sql("SELECT * FROM SYSIBM.SYSCOLUMNS WHERE TBNAME = '#{im.call(table)}'").
          collect do |column| 
            column[:db_type]     = column.delete(:typename)
            if column[:db_type]  == "DECIMAL"
              # Cannot tell from :scale the actual scale number, but should be
              # sufficient to identify integers
              column[:db_type] << "(#{column[:longlength]},#{column[:scale] ? 1 : 0})"
            end
            column[:allow_null]  = column.delete(:nulls) == 'Y'
            column[:primary_key] = column.delete(:identity) == 'Y' || !column[:keyseq].nil?
            column[:type]        = schema_column_type(column[:db_type])
            [ m.call(column.delete(:name)), column]
          end
      end

      def tables
        metadata_dataset.with_sql("SELECT TABNAME FROM SYSCAT.TABLES WHERE TYPE='T' AND OWNER = '#{opts[:user].upcase}'").
          all.map{|h| output_identifier_meth.call(h[:tabname]) }
      end

      def views
        metadata_dataset.with_sql("SELECT TABNAME FROM SYSCAT.TABLES WHERE TYPE='V' AND OWNER = '#{opts[:user].upcase}'").
          all.map{|h| output_identifier_meth.call(h[:tabname]) }
      end

      def indexes(table, opts = {})
        metadata_dataset.
          with_sql("select indname, uniquerule, made_unique, system_required from SYSCAT.INDEXES where TABNAME = '#{table.to_s.upcase}'").
          all.map{|h| Hash[ h.map{|k,v| [k.to_sym, v]} ] }
      end

      private

      # db2 specific alter table
      def alter_table_sql(table, op)
        case op[:op]
        when :add_column
          "ALTER TABLE #{quote_schema_table(table)} ADD #{column_definition_sql(op)}"
        when :drop_column
          "ALTER TABLE #{quote_schema_table(table)} DROP #{column_definition_sql(op)}"
        when :rename_column       # renaming is only possible after db2 v9.7
          "ALTER TABLE #{quote_schema_table(table)} RENAME COLUMN #{quote_identifier(op[:name])} TO #{quote_identifier(op[:new_name])}"
        when :set_column_type
          "ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{quote_identifier(op[:name])} SET DATA TYPE #{type_literal(op)}"
        when :set_column_default
          "ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{quote_identifier(op[:name])} SET DEFAULT #{literal(op[:default])}"
        else
          super(table, op)
        end
      end

      # db2 specific autoincrement
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
          "DECLARE GLOBAL TEMPORARY TABLE #{options[:temp] ? quote_identifier(name) : quote_schema_table(name)} (#{column_list_sql(generator)})"
        else
          super
        end
      end

      # DB2 has issues with quoted identifiers, so
      # turn off database quoting by default.
      def quote_identifiers_default
        false
      end

      def rename_table_sql(name, new_name)
        "RENAME TABLE #{quote_schema_table(name)} TO #{quote_schema_table(new_name)}"
      end

    end

    module DatasetMethods
      BITWISE_METHOD_MAP = {:& =>:BITAND, :| => :BITOR, :^ => :BITXOR, :'B~'=>:BITNOT}
      # db2 supplies CURRENT_TIMESTAMP in local time instead of utc
      CONSTANT_MAP = {:CURRENT_TIMESTAMP=>"CURRENT_TIMESTAMP - CURRENT_TIMEZONE".freeze}
      
      def cast_sql(expr, type)
        type == String ?  "RTRIM(CHAR(#{literal(expr)}))" : super
      end

      # Work around DB2's lack of a case insensitive LIKE operator
      def complex_expression_sql(op, args)
        case op
        when :ILIKE
          super(:LIKE, [SQL::Function.new(:upper, args.at(0)), SQL::Function.new(:upper, args.at(1)) ])
        when :"NOT ILIKE"
          super(:"NOT LIKE", [SQL::Function.new(:upper, args.at(0)), SQL::Function.new(:upper, args.at(1)) ])
        when :&, :|, :^, :'B~'
          # works with db2 v9.5 and after
          literal(SQL::Function.new(BITWISE_METHOD_MAP[op], *args))
        when :<<
          "(#{literal(args[0])} * POWER(2, #{literal(args[1])}))"
        when :>>
          "(#{literal(args[0])} / POWER(2, #{literal(args[1])}))"
        else
          super(op, args)
        end
      end

      def constant_sql(constant)
        CONSTANT_MAP[constant] || super
      end

      # Add a fallback table for empty from situation
      def get(*args)
        @opts[:from] ? super : from(:sysibm__sysdummy1).get(*args)
      end

      def supports_is_true?
        false
      end

      def supports_multiple_column_in?
        false
      end

      def supports_timestamp_usecs?
        false
      end

      # DB2 supports window functions
      def supports_window_functions?
        true
      end

      def supports_where_true?
        false
      end

      private

      def convert_type(v, type)
        case type
        when :time;
          Sequel::string_to_time v
        when :date;
          Sequel::string_to_date v
        when :timestamp;
          Sequel::database_to_application_timestamp v
        when :int;
          v.to_i
        when :boolean;
          v.to_i.zero? ? false : true
        when :blob, :clob;
          v.to_sequel_blob
        else;
          v
        end
      end

      # DB2 uses "INSERT INTO "ITEMS" VALUES DEFAULT" for a record with default values to be inserted
      def insert_values_sql(sql)
        opts[:values].empty? ? sql << " VALUES DEFAULT" : super
      end

      # Modify the sql to limit the number of rows returned
      # Note: 
      #
      #     After db2 v9.7, MySQL flavored "LIMIT X OFFSET Y" can be enabled using
      #
      #     db2set DB2_COMPATIBILITY_VECTOR=MYS
      #     db2stop
      #     db2start
      #
      #     Support for this feature is not used in this adapter however.
      def select_limit_sql(sql)
        if l = @opts[:limit]
          sql << " FETCH FIRST #{l == 1 ? 'ROW' : "#{literal(l)} ROWS"} ONLY"
        end
      end

      # Modify the sql to limit the number of rows returned
      # Note: 
      #
      #     After db2 v9.7, MySQL flavored "LIMIT X OFFSET Y" can be enabled using
      #
      #     db2set DB2_COMPATIBILITY_VECTOR=MYS
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
        "ALTER TABLE #{table} ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
      end
    end
  end
end
