require 'ibm_db'
Sequel.require 'adapters/shared/db2'

module Sequel

  module IBMDB
    @convert_smallint_to_bool = true
    @use_clob_as_blob = true

    class << self
      attr_accessor :convert_smallint_to_bool
      attr_accessor :use_clob_as_blob
    end

    class Connection
      attr_accessor :prepared_statements

      def initialize(connection_string)
        @conn = IBM_DB.connect(connection_string, '', '')
        @prepared_statements = {}
      end

      def autocommit
        IBM_DB.autocommit(@conn) == 1
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
          raise Sequel::DatabaseError, error_msg
        end
      end

      def execute(sql)
        Statement.new(IBM_DB.exec(@conn, sql))
      end

      def execute_prepared(ps_name, *values)
        stmt = @prepared_statements[ps_name].last
        res = stmt.execute(*values)
        unless res
          raise Sequel::DatabaseError, "Error executing statement #{ps_name}: #{stmt.error_msg} "
        end
        stmt
      end

      def error_msg
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

      def error_msg
        IBM_DB.getErrormsg(@stmt, 0)
      end

      def fail?
        ! @stmt
      end

      def fetch_assoc
        IBM_DB.fetch_assoc(@stmt)   if @stmt
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
      include Sequel::DB2::DatabaseMethods

      set_adapter_scheme :ibmdb

      def alter_table(name, generator=nil, &block)
        res = super
        reorg(name)   # db2 needs to reorg
        res
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

      def dataset(opts = nil)
        IBMDB::Dataset.new(self, opts)
      end

      def execute(sql, opts={}, &block)
        if sql.is_a?(Symbol)
          execute_prepared_statement(sql, opts, &block)
        else
          synchronize(opts[:server]){|c| _execute(c, sql, opts, &block)}
        end
      rescue RuntimeError => e
        raise_error(e)
      end

      def execute_insert(sql, opts={}, &block)
        execute_dui(sql, opts, &block)
        metadata_dataset.with_sql("SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1").get
      end

      def execute_prepared_statement(ps_name, opts)
        args = opts[:arguments]
        ps = prepared_statements[ps_name]
        sql = ps.prepared_sql
        synchronize(opts[:server]) do |conn|
          unless conn.prepared_statements.fetch(ps_name, []).first == sql
            conn.prepare(sql, ps_name)
          end
          # use eval to remote outer-most quotes for strings and convert float
          # and ingteger back to their ruby types
          args.map!{|v| v.nil? ? nil : eval(literal(v))}
          stmt = conn.execute_prepared(ps_name, *args)

          block_given? ? yield(stmt): stmt.affected
        end
      end

      # Convert smallint type to boolean if convert_smallint_to_bool is true
      def schema_column_type(db_type)
        if Sequel::IBMDB.convert_smallint_to_bool && db_type =~ /smallint/i 
          :boolean
        else
          db_type =~ /[bc]lob/i ? :blob : super
        end
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

      private

      def begin_transaction(conn, opts={})
        log_yield(TRANSACTION_BEGIN){conn.autocommit = false}
        conn
      end

      def disconnect_connection(conn)
        conn.close
      end

      # We uses the clob type by default for Files.
      # Note: if user select to use blob, then insert statement should use 
      # use this for blob value:
      #     cast(X'fffefdfcfbfa' as blob(2G))
      def type_literal_generic_file(column)
        IBMDB::use_clob_as_blob ? :clob : :blob
      end

      def type_literal_generic_trueclass(column)
        :smallint
      end
      alias_method :type_literal_generic_falseclass, :type_literal_generic_trueclass

      # DB2 does not have a special type for text
      def type_literal_specific(column)
        column[:type] == :text ? "varchar(#{column[:size]||255})" : super
      end
      
      def remove_transaction(conn)
        conn.autocommit = true if conn
        super
      end
    
      def reorg(table)
        synchronize(opts[:server]){|c| c.reorg(table)}
      end

      def _execute(conn, sql, opts)
        stmt = log_yield(sql){ conn.execute(sql) }
        raise Sequel::DatabaseError, conn.error_msg if stmt.fail?
        block_given? ? yield(stmt) : stmt.affected
      end

    end
    
    class Dataset < Sequel::Dataset
      include Sequel::DB2::DatasetMethods

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
          columns = []
          column_info = {}
          stmt.num_fields.times do |i|
            k = stmt.field_name i
            key = output_identifier(k)
            column_info[key] = output_identifier(stmt.field_type k)
            # decide if it is a smallint from precision
            column_info[key] = :boolean  if IBMDB::convert_smallint_to_bool and column_info[key] == :int and stmt.field_precision(k) < 8
            columns << key
          end
          @columns = columns

          while res = stmt.fetch_assoc
            #yield res
            row = {}
            res.each do |k, v|
              key = output_identifier(k)
              row[key] = v.nil? ? v : convert_type(v, column_info[key])
            end
            yield row
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

    end
  end
end
