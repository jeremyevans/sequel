require 'ibm_db'

module IBM_DB
  class Connection
    attr_accessor :stmt   # the IBM_DB statement holder
    attr_accessor :prepared_statements, :prepared_db_stmt

    def free_stmt
      IBM_DB.free_result(@stmt)
      @stmt = nil
    end

    def fetch_stmt_assoc
      return nil  unless @stmt
      IBM_DB.fetch_assoc(@stmt)
    end

    def close
      IBM_DB.close(self)
    end

    def prepare(sql, ps_name)
      if stmt = IBM_DB.prepare(self, sql)
        ps_name = ps_name.to_sym
        @prepared_statements[ps_name] = sql
        @prepared_db_stmt[ps_name] = stmt
      else
        raise get_error_msg
      end
    end

    def bind(ps, *variables)
      stmt = @prepared_db_stmt[ps]
      variables.each.with_index do |v, i|
        res = IBM_DB.bind_param(stmt, i + 1 )
        raise "Binding variable to statement #{ps} failed" unless res
      end
    end

    def execute(sql)
      @stmt = IBM_DB.exec(self, sql)
      self
    end

    def execute_prepared(ps_name, *values)
      @stmt = @prepared_db_stmt[ps_name]
      res = IBM_DB.execute(@stmt, values)
      raise "Error executing statement #{ps_name} " unless res
    end

    def stmt_affected
      IBM_DB.num_rows(@stmt)
    end

    def stmt_field_type(key)
      IBM_DB.field_type(@stmt, key)
    end

    def stmt_num_fields
      IBM_DB.num_fields(@stmt)
    end

    def stmt_field_name(ind)
      IBM_DB.field_name(@stmt, ind)
    end

    def get_error_msg
      IBM_DB.getErrormsg(self, IBM_DB::DB_CONN)
    end
  end
end

module Sequel
  module IBMDB
    class Database < Sequel::Database
      set_adapter_scheme :ibmdb

      AUTOINCREMENT = 'GENERATED ALWAYS AS IDENTITY'.freeze


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

        conn = IBM_DB.connect(connection_string, '', '')
        conn.prepared_statements = {}
        conn.prepared_db_stmt = {}

        conn
      end
      
      def test_connection(server=nil)
        synchronize(server){|conn|}
        true
      end

      def dataset(opts = nil)
        IBMDB::Dataset.new(self, opts)
      end
      
      # Returns number of rows affected
      def execute_dui(sql, opts={}) 
        if sql.is_a?(Symbol)
          execute_prepared_statement(sql, opts, &block)
        else
          synchronize(opts[:server]){|c| c = _execute(c, sql, opts); c.stmt_affected}
        end
      end
      alias_method :do, :execute_dui
      
      def execute(sql, opts={}, &block)
        if sql.is_a?(Symbol)
          execute_prepared_statement(sql, opts, &block)
        else
          synchronize(opts[:server]){|c| yield _execute(c, sql, opts)}
        end
      end
      alias_method :query, :execute

      def execute_prepared_statement(ps_name, opts, &block)
        args = opts[:arguments]
        ps = prepared_statements[ps_name]
        sql = ps.prepared_sql
        synchronize(opts[:server]) do |conn|
          unless conn.prepared_statements[ps_name] == sql
            conn.prepare(sql, ps_name)
          end
          conn.execute_prepared(ps_name, *args)

          yield conn
        end
      end

      def tables
        metadata_dataset.with_sql("select TABNAME from SYSCAT.TABLES where type='T'").
          all.map{|h| h[:tabname].downcase.to_sym }
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
      def auto_increment_sql
        AUTOINCREMENT
      end

      def _execute(conn, sql, opts)
        conn = log_yield(sql){ conn.execute(sql) }
        raise conn.get_error_msg unless conn.stmt
        conn
      end
      
      def disconnect_connection(conn)
        conn.close
      end
    end
    
    class Dataset < Sequel::Dataset
      
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
        
        # Same as execute, explicit due to intricacies of alias and super.
        def execute_dui(sql, opts={}, &block)
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
        def set_columns(conn)
          @column_info = {}
          @columns = []
          conn.stmt_num_fields.times do |i|
            k = conn.stmt_field_name i
            key = output_identifier(k)
            @column_info[key] = output_identifier(conn.stmt_field_type k)
            @columns << key
          end
        end

        execute(sql) do |conn|
          break unless conn.stmt
          set_columns(conn) unless @columns

          while res = conn.fetch_stmt_assoc
            #yield res
            yield hash_row(res)
          end
          conn.free_stmt
        end
        self
      end

      # DB2 supports window functions
      def supports_window_functions?
        true
      end

      def supports_prepared_transactions?
        true
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

      # Modify the sql to limit the number of rows returned
      def select_limit_sql(sql)
        if l = @opts[:limit]
          sql << " FETCH FIRST #{l == 1 ? 'ROW' : "#{literal(l)} ROWS"} ONLY"
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
      
      def convert_type(v, type)
        case type
        # conversioin of time to ruby +Time+ object is not used here, because the
        # automatically added date part can be misleading and dangerous
        #when :time;
            #Time.parse(v)
        when :date; 
            Date.parse(v)
        when :timestamp; 
            DateTime.parse(v)
        else
            v
        end
      end
    end
  end
end
