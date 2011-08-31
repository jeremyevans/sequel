require 'ibm_db'

module Sequel
  module IBMDB
    @convert_smallint_to_bool = true

    class << self
      attr_accessor :convert_smallint_to_bool
    end

    class Connection
      attr_accessor :prepared_statements

      def initialize(connection_string)
        @conn = IBM_DB.connect(connection_string, '', '')
        @prepared_statements = {}
      end

      def autocommit=(value)
        state = value ? IBM_DB::SQL_AUTOCOMMIT_ON : IBM_DB::SQL_AUTOCOMMIT_OFF 
        IBM_DB.autocommit(@conn, state)
      end

      def close
        IBM_DB.close(@conn)
      end

      def prepare(sql, ps_name)
        if stmt = IBM_DB.prepare(@conn, sql)
          ps_name = ps_name.to_sym
          stmt = Statement.new(stmt)
          @prepared_statements[ps_name] = [sql, stmt]
        else
          raise Sequel::DatabaseError, get_error_msg
        end
      end

      def execute(sql)
        Statement.new(IBM_DB.exec(@conn, sql))
      end

      def execute_prepared(ps_name, *values)
        stmt = @prepared_statements[ps_name].last
        res = stmt.execute(*values)
        raise Sequel::DatabaseError, "Error executing statement #{ps_name} " unless res
        stmt
      end

      def get_error_msg
        IBM_DB.getErrormsg(@conn, IBM_DB::DB_CONN)
      end

      def reorg(table)
        execute("CALL ADMIN_CMD('REORG TABLE #{table}')")
      end

    end

    class Statement
      def initialize(stmt)
        @stmt = stmt
      end

      def affected
        IBM_DB.num_rows(@stmt)
      end

      def execute(*values)    # values are for prepared statement
        IBM_DB.execute(@stmt, values)
      end

      def fail?
        return true   unless @stmt
      end

      def fetch_assoc
        return nil  unless @stmt
        IBM_DB.fetch_assoc(@stmt)
      end
      def field_name(ind)
        IBM_DB.field_name(@stmt, ind)
      end

      def field_type(key)
        IBM_DB.field_type(@stmt, key)
      end

      # use this one to determine if is a smallint
      def field_precision(key)
        IBM_DB.field_precision(@stmt, key)
      end

      def free
        IBM_DB.free_result(@stmt)
      end

      def num_fields
        IBM_DB.num_fields(@stmt)
      end
    end

    class Database < Sequel::Database
      set_adapter_scheme :ibmdb

      PRIMARY_KEY   = ' NOT NULL PRIMARY KEY'.freeze
      AUTOINCREMENT = 'GENERATED ALWAYS AS IDENTITY'.freeze
      NULL          = ''.freeze

      def alter_table(name, generator=nil, &block)
        super
        reorg(name)   # db2 needs to reorg
        nil
      end
    
      def connect(server)
        opts = server_opts(server)
        
        # use uncataloged connection so that host and port can be supported
        connection_string = ( \
            'Driver={IBM DB2 ODBC DRIVER};' \
            "Database=#{opts[:database]};" \
            "Hostname=#{opts[:host]};" \
            "Port=#{opts[:port] || 50000};" \
            'Protocol=TCPIP;' \
            "Uid=#{opts[:user]};" \
            "Pwd=#{opts[:password]};" \
        )

        Connection.new(connection_string)
      end

      def database_type
        :db2
      end
      
      def dataset(opts = nil)
        IBMDB::Dataset.new(self, opts)
      end
      
      def execute(sql, opts={}, &block)
        if sql.is_a?(Symbol)
          execute_prepared_statement(sql, opts, &block)
        else
          synchronize(opts[:server]){|c| _execute(c, sql, opts, &block)}
        end
      end

      def execute_dui(sql, opts={}, &block)
        if sql.is_a?(Symbol)
          execute_prepared_statement(sql, opts, &block)
        else
          synchronize(opts[:server]){|c| _execute(c, sql, opts, &block)}
        end
      end

      def execute_prepared_statement(ps_name, opts)
        args = opts[:arguments]
        ps = prepared_statements[ps_name]
        sql = ps.prepared_sql
        synchronize(opts[:server]) do |conn|
          unless conn.prepared_statements[ps_name] == sql
            conn.prepare(sql, ps_name)
          end
          stmt = conn.execute_prepared(ps_name, *args)

          block_given? ? yield(stmt): stmt.affected
        end
      end

      # Convert smallint type to boolean if convert_smallint_to_bool is true
      def schema_column_type(db_type)
        Sequel::IBMDB.convert_smallint_to_bool && db_type =~ /smallint/i ?  :boolean : super
      end

      def schema_parse_table(table, opts = {})
        m = output_identifier_meth
        im = input_identifier_meth
        metadata_dataset.with_sql("select * from SYSIBM.SYSCOLUMNS where TBNAME = '#{im.call(table)}'").
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
        metadata_dataset.with_sql("select TABNAME from SYSCAT.TABLES where type='T'").
          all.map{|h| h[:tabname].downcase.to_sym }
      end

      def table_exists?(name)
        sch, table_name = schema_and_table(name)
        name = SQL::QualifiedIdentifier.new(sch, table_name) if sch
        from(name).first
        true
      rescue Exception => e
        # table needs reorg
        if e.to_s =~ /Operation not allowed for reason code "7" on table/
          reorg(name)
          retry
        end
        false
      end

      def views
        metadata_dataset.with_sql("select TABNAME from SYSCAT.TABLES where type='V'").
          all.map{|h| h[:tabname].downcase.to_sym }
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
        # bypass null/not null fragment if primary_key is set
        return  if column[:primary_key]

        null = column.fetch(:null, column[:allow_null])
        sql << NOT_NULL if null == false
        sql << NULL if null == true
      end

      # Add primary key SQL fragment to column creation SQL.
      def column_definition_primary_key_sql(sql, column)
        sql << PRIMARY_KEY if column[:primary_key]
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

      def disconnect_connection(conn)
        conn.close
      end

      def type_literal_generic_trueclass(column)
        :smallint
      end

      def type_literal_generic_falseclass(column)
        type_literal_generic_trueclass(column)
      end

      # DB2 does not have a special type for text
      def type_literal_specific(column)
        column[:type] == :text ? "varchar(#{column[:size]||255})" : super
      end
    
      def rename_table_sql(name, new_name)
        "RENAME TABLE #{quote_schema_table(name)} TO #{quote_schema_table(new_name)}"
      end

      def reorg(table)
        synchronize(opts[:server]){|c| c.reorg(table)}
      end

      def begin_transaction(conn, opts={})
        log_yield(TRANSACTION_BEGIN){conn.autocommit = false}
        conn
      end
      
      def remove_transaction(conn)
        conn.autocommit = true if conn
        super
      end

      def _execute(conn, sql, opts)
        stmt = log_yield(sql){ conn.execute(sql) }
        raise Sequel::DatabaseError, conn.get_error_msg if stmt.fail?
        block_given? ? yield(stmt) : stmt.affected
      end
      
    end
    
    class Dataset < Sequel::Dataset
      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      
      module CallableStatementMethods
        # Extend given dataset with this module so subselects inside subselects in
        # prepared statements work.
        def subselect_sql(ds)
          ps = ds.to_prepared_statement(:select)
          ps.extend(CallableStatementMethods)
          ps = ps.bind(@opts[:bind_vars]) if @opts[:bind_vars]
          ps.prepared_args = prepared_args
          ps.prepared_sql
        end
      end
      
      # Methods for DB2 prepared statements using the native driver.
      module PreparedStatementMethods
        include Sequel::Dataset::UnnumberedArgumentMapper
        
        private
        # Execute the prepared statement with the bind arguments instead of
        # the given SQL.
        def execute(sql, opts={}, &block)
          super(prepared_statement_name, {:arguments=>bind_arguments}.merge(opts), &block)
        end
        
        def execute_dui(sql, opts={}, &block)
          super(prepared_statement_name, {:arguments=>bind_arguments}.merge(opts), &block)
        end

        # Execute the given SQL on the database using execute_insert.
        def execute_insert(sql, opts={}, &block)
          super(prepared_statement_name, {:arguments=>bind_arguments}.merge(opts), &block)
        end

      end
      
      # emulate level of support of bind arguments same to MySQL
      def call(type, bind_arguments={}, *values, &block)
        ps = to_prepared_statement(type, values)
        ps.extend(CallableStatementMethods)
        ps.call(bind_arguments, &block)
      end

      def fetch_rows(sql)
        execute(sql) do |stmt|
          break if stmt.fail?
          unless @columns and @column_info
            @column_info = {}
            @columns = []
            stmt.num_fields.times do |i|
              k = stmt.field_name i
              key = output_identifier(k)
              @column_info[key] = output_identifier(stmt.field_type k)
              if IBMDB::convert_smallint_to_bool and @column_info[key] == :int
                precision = stmt.field_precision k
                @column_info[key] = :boolean  if precision < 8
              end
              @columns << key
            end
          end

          while res = stmt.fetch_assoc
            #yield res
            yield hash_row(res)
          end
          stmt.free
        end
        self
      end

      # Store the given type of prepared statement in the associated database
      # with the given name.
      def prepare(type, name=nil, *values)
        ps = to_prepared_statement(type, values)
        ps.extend(PreparedStatementMethods)
        if name
          ps.prepared_statement_name = name
          db.prepared_statements[name] = ps
        end
        ps
      end

      def supports_timestamp_usecs?
        false
      end

      # DB2 supports window functions
      def supports_window_functions?
        true
      end

      private

      def convert_type(v, type)
        case type
        when :time;
          Sequel::SQLTime.parse(v)
        when :date;
          Date.parse(v)
        when :timestamp;
          DateTime.parse(v)
        when :int;
          v.to_i
        when :boolean;
          v.to_i.zero? ? false : true
        else;
          v
        end
      end

      def hash_row(sth)
        row = {}
        sth.each do |k, v|
            key = output_identifier(k)
            row[key] = v.nil? ? v : convert_type(v, @column_info[key])
        end
        row
      end
      
      # DB2 uses "INSERT INTO "ITEMS" VALUES DEFAULT" for a record with default values to be inserted
      def insert_values_sql(sql)
        opts[:values].empty? ? sql << " VALUES DEFAULT" : super
      end

      # Modify the sql to limit the number of rows returned
      def select_limit_sql(sql)
        if l = @opts[:limit]
          sql << " FETCH FIRST #{l == 1 ? 'ROW' : "#{literal(l)} ROWS"} ONLY"
        end
      end

      # Use 0 for false on DB2
      def literal_false
        BOOL_FALSE
      end

      # Use 1 for true on DB2
      def literal_true
        BOOL_TRUE
      end
      
      def _truncate_sql(table)
        "TRUNCATE #{table} IMMEDIATE"
      end
    end
  end
end
