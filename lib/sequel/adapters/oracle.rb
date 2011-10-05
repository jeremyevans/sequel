require 'oci8'
Sequel.require 'adapters/shared/oracle'

module Sequel
  module Oracle
    class Database < Sequel::Database
      include DatabaseMethods
      set_adapter_scheme :oracle

      # ORA-00028: your session has been killed
      # ORA-01012: not logged on
      # ORA-03113: end-of-file on communication channel
      # ORA-03114: not connected to ORACLE
      CONNECTION_ERROR_CODES = [ 28, 1012, 3113, 3114 ]      
      
      def connect(server)
        opts = server_opts(server)
        if opts[:database]
          dbname = opts[:host] ? \
            "//#{opts[:host]}#{":#{opts[:port]}" if opts[:port]}/#{opts[:database]}" : opts[:database]
        else
          dbname = opts[:host]
        end
        conn = OCI8.new(opts[:user], opts[:password], dbname, opts[:privilege])
        conn.autocommit = true
        conn.non_blocking = true
        
        # The ruby-oci8 gem which retrieves oracle columns with a type of
        # DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE is complex based on the
        # ruby version (1.9.2 or later) and Oracle version (9 or later)
        # In the now standard case of 1.9.2 and Oracle 9 or later, the timezone
        # is determined by the Oracle session timezone. Thus if the user
        # requests Sequel provide UTC timezone to the application,
        # we need to alter the session timezone to be UTC
        if Sequel.application_timezone == :utc
          conn.exec("ALTER SESSION SET TIME_ZONE='-00:00'")
        end
        
        conn
      end
      
      def dataset(opts = nil)
        Oracle::Dataset.new(self, opts)
      end

      def schema_parse_table(table, opts={})
        schema, table = schema_and_table(table)
        schema ||= opts[:schema]
        schema_and_table = "#{"#{quote_identifier(opts[:schema])}." if opts[:schema]}#{quote_identifier(table)}"
        table_schema = []
        m = output_identifier_meth
        im = input_identifier_meth
        ds = metadata_dataset.from(:all_constraints___cons, :all_cons_columns___cols).
          where(:cols__table_name=>im.call(table), :cons__constraint_type=>'P',
                :cons__constraint_name=>:cols__constraint_name, :cons__owner=>:cols__owner)
        ds = ds.where(:cons__owner=>im.call(opts[:schema])) if opts[:schema]
        pks = ds.select_map(:cols__column_name)
        metadata = transaction(opts){|conn| log_yield("Connection.describe_table"){conn.describe_table(schema_and_table)}}
        metadata.columns.each do |column|
          table_schema << [
            m.call(column.name),
            {
              :primary_key => pks.include?(column.name),
              :type => column.data_type,
              :db_type => column.type_string.split(' ')[0],
              :type_string => column.type_string,
              :charset_form => column.charset_form,
              :char_used => column.char_used?,
              :char_size => column.char_size,
              :data_size => column.data_size,
              :precision => column.precision,
              :scale => column.scale,
              :fsprecision => column.fsprecision,
              :lfprecision => column.lfprecision,
              :allow_null => column.nullable?
            }
          ]
        end
        table_schema
      end

      def execute(sql, opts={})
        synchronize(opts[:server]) do |conn|
          begin
            r = log_yield(sql){conn.exec(sql)}
            yield(r) if block_given?
            r
          rescue OCIException => e
            raise_error(e)
          end
        end
      end
      alias_method :do, :execute

      private
      
      def begin_transaction(conn, opts={})
        log_yield(TRANSACTION_BEGIN){conn.autocommit = false}
        conn
      end
      
      def commit_transaction(conn, opts={})
        log_yield(TRANSACTION_COMMIT){conn.commit}
      end

      def disconnect_connection(c)
        c.logoff
      rescue OCIInvalidHandle
        nil
      end

      def disconnect_error?(e, opts)
        super || (e.is_a?(::OCIException) && CONNECTION_ERROR_CODES.include?(e.code))
      end
      
      def remove_transaction(conn)
        conn.autocommit = true if conn
        super
      end
      
      def rollback_transaction(conn, opts={})
        log_yield(TRANSACTION_ROLLBACK){conn.rollback}
      end
    end
    
    class Dataset < Sequel::Dataset
      include DatasetMethods

      def fetch_rows(sql)
        execute(sql) do |cursor|
          begin
            @columns = cursor.get_col_names.map{|c| output_identifier(c)}
            while r = cursor.fetch
              row = {}
              r.each_with_index {|v, i| row[@columns[i]] = v unless @columns[i] == :raw_rnum_}
              yield row
            end
          ensure
            cursor.close
          end
        end
        self
      end

      private

      def literal_other(v)
        case v
        when OraDate
          literal(db.to_application_timestamp(v))
        when OCI8::CLOB
          v.rewind
          literal(v.read)
        else
          super
        end
      end
    end
  end
end
