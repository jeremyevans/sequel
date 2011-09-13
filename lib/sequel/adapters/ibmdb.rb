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

    tt = Class.new do
      def boolean(s) !s.to_i.zero? end
      def int(s) s.to_i end
    end.new
    DB2_TYPES = {
      :boolean => tt.method(:boolean),
      :int => tt.method(:int),
      :blob => ::Sequel::SQL::Blob.method(:new),
      :time => ::Sequel.method(:string_to_time),
      :date => ::Sequel.method(:string_to_date),
      :timestamp => ::Sequel.method(:database_to_application_timestamp)
    }
    DB2_TYPES[:clob] = DB2_TYPES[:blob]

    class Connection
      attr_accessor :prepared_statements

      # Error class for exceptions raised by the connection.
      class Error < StandardError
      end

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

      def commit
        IBM_DB.commit(@conn)
      end
      def rollback
        IBM_DB.rollback(@conn)
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
          err = error_msg
          err = "Error preparing #{ps_name} with SQL: #{sql}" if error_msg.nil? || error_msg.empty?
          raise Error, err
        end
      end

      def execute(sql)
        Statement.new(IBM_DB.exec(@conn, sql))
      end

      def execute_prepared(ps_name, *values)
        stmt = @prepared_statements[ps_name].last
        res = stmt.execute(*values)
        unless res
          raise Error, "Error executing statement #{ps_name}: #{error_msg}"
        end
        stmt
      end

      def error_msg
        IBM_DB.getErrormsg(@conn, IBM_DB::DB_CONN)
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
        !@stmt
      end

      def fetch_array
        IBM_DB.fetch_array(@stmt) if @stmt
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
        reorg(name)
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
      rescue Connection::Error => e
        raise_error(e)
      end

      def execute_insert(sql, opts={})
        synchronize(opts[:server]) do |c|
          if sql.is_a?(Symbol)
            execute_prepared_statement(sql, opts)
          else
            _execute(c, sql, opts)
          end
          _execute(c, "SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1", opts){|stmt| stmt.fetch_array.first.to_i}
        end
      rescue Connection::Error => e
        raise_error(e)
      end

      def execute_prepared_statement(ps_name, opts)
        args = opts[:arguments]
        ps = prepared_statements[ps_name]
        sql = ps.prepared_sql
        synchronize(opts[:server]) do |conn|
          unless conn.prepared_statements.fetch(ps_name, []).first == sql
            log_yield("Preparing #{ps_name}: #{sql}"){conn.prepare(sql, ps_name)}
          end
          # use eval to remote outer-most quotes for strings and convert float
          # and ingteger back to their ruby types
          args = args.map{|v| v.nil? ? nil : prepared_statement_arg(v)}
          stmt = log_yield("Executing #{ps_name}: #{args.inspect}"){conn.execute_prepared(ps_name, *args)}

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
        v ||= false # only retry once
        sch, table_name = schema_and_table(name)
        name = SQL::QualifiedIdentifier.new(sch, table_name) if sch
        from(name).first
        true
      rescue DatabaseError => e
        if e.to_s =~ /Operation not allowed for reason code "7" on table/ && v == false
          # table probably needs reorg
          reorg(name)
          v = true
          retry 
        end
        false
      end

      private

      def begin_transaction(conn, opts={})
        log_yield(TRANSACTION_BEGIN){conn.autocommit = false}
        conn
      end

      def rollback_transaction(conn, opts={})
        log_yield(TRANSACTION_ROLLBACK){conn.rollback} if conn
      ensure
        conn.autocommit = true if conn
      end

      def commit_transaction(conn, opts={})
        log_yield(TRANSACTION_COMMIT){conn.commit} if conn
      ensure
        conn.autocommit = true if conn
      end
    
      def disconnect_connection(conn)
        conn.close
      end

      def prepared_statement_arg(v)
        case v
        when Numeric
          v.to_s
        when Date, Time
          literal(v).gsub("'", '')
        else
          v
        end
      end

      def metadata_dataset
        ds = super
        ds.convert_smallint_to_bool = false
        ds
      end

      def _execute(conn, sql, opts)
        stmt = log_yield(sql){ conn.execute(sql) }
        raise Connection::Error, conn.error_msg if stmt.fail?
        if block_given?
          begin
            yield(stmt)
          ensure
            stmt.free
          end
        else  
          stmt.affected
        end
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

      def convert_smallint_to_bool
        defined?(@convert_smallint_to_bool) ? @convert_smallint_to_bool : (@convert_smallint_to_bool = IBMDB.convert_smallint_to_bool)
      end
      attr_writer :convert_smallint_to_bool

      def fetch_rows(sql)
        execute(sql) do |stmt|
          break if stmt.fail?
          offset = @opts[:offset]
          columns = []
          convert = convert_smallint_to_bool
          stmt.num_fields.times do |i|
            k = stmt.field_name i
            key = output_identifier(k)
            type = stmt.field_type(k).downcase.to_sym
            # decide if it is a smallint from precision
            type = :boolean  if type ==:int && convert && stmt.field_precision(k) < 8
            columns << [key, DB2_TYPES[type]]
          end
          cols = columns.map{|c| c.at(0)}
          cols.delete(row_number_column) if offset
          @columns = cols

          while res = stmt.fetch_array
            row = {}
            res.zip(columns).each do |v, (k, pr)|
              row[k] = ((pr ? pr.call(v) : v) if v)
            end
            row.delete(row_number_column) if offset
            yield row
          end
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
    end
  end
end
