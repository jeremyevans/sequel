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
      
      ORACLE_TYPES = {}

      # Hash of conversion procs for this database.
      attr_reader :conversion_procs

      def initialize(opts={})
        super
        @autosequence = opts[:autosequence]
        @primary_key_sequences = {}
        @conversion_procs = ORACLE_TYPES.dup
      end

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

        # Primary Keys
        ds = metadata_dataset.from(:all_constraints___cons, :all_cons_columns___cols).
          where(:cols__table_name=>im.call(table), :cons__constraint_type=>'P',
                :cons__constraint_name=>:cols__constraint_name, :cons__owner=>:cols__owner)
        ds = ds.where(:cons__owner=>im.call(opts[:schema])) if opts[:schema]
        pks = ds.select_map(:cols__column_name)

        # Default values
        defaults =  metadata_dataset.from(:dba_tab_cols).
          where(:table_name=>im.call(table)).
          to_hash(:column_name, :data_default)

        metadata = synchronize(opts[:server]) do |conn|
          begin
          log_yield("Connection.describe_table"){conn.describe_table(schema_and_table)}
          rescue OCIError => e
            raise_error(e)
          end
        end
        metadata.columns.each do |column|
          h = {
              :primary_key => pks.include?(column.name),
              :default => defaults[column.name],
              :oci8_type => column.data_type,
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
          h[:type] = oracle_column_type(h)
          table_schema << [m.call(column.name), h]
        end
        table_schema
      end
      
      def oracle_column_type(h)
        case h[:oci8_type]
        when :number
          case h[:scale]
          when 0
            :integer
          when -127
            :float
          else
            :decimal
          end
        else
          schema_column_type(h[:db_type])
        end
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
      alias do execute

      def execute_insert(sql, opts={})
        synchronize(opts[:server]) do |conn|
          begin
            log_yield(sql){conn.exec(sql)}
            unless sequence = opts[:sequence]
              if t = opts[:table]
                sequence = sequence_for_table(t)
              end
            end
            if sequence
              sql = "SELECT #{literal(sequence)}.currval FROM dual"
              begin
                cursor = log_yield(sql){conn.exec(sql)}
                row = cursor.fetch
                row.each{|v| return (v.to_i if v)}
              rescue OCIError
                nil
              ensure
                cursor.close if cursor
              end
            end
          rescue OCIException => e
            raise_error(e)
          end
        end
      end

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

      def sequence_for_table(table)
        return nil unless autosequence
        @primary_key_sequences.fetch(table) do |key|
          pk = schema(table).select{|k, v| v[:primary_key]}
          seq = if pk.length == 1
            :"seq_#{table}_#{pk.first.first}"
          end
          @primary_key_sequences[table] = seq
        end
      end
    end
    
    class Dataset < Sequel::Dataset
      include DatasetMethods

      def fetch_rows(sql)
        execute(sql) do |cursor|
          begin
            offset = @opts[:offset]
            rn = row_number_column
            cps = db.conversion_procs
            cols = columns = cursor.get_col_names.map{|c| output_identifier(c)}
            metadata = cursor.column_metadata
            cm = cols.zip(metadata).map{|c, m| [c, cps[m.data_type]]}
            columns = cols.reject{|x| x == rn} if offset
            @columns = columns
            while r = cursor.fetch
              row = {}
              r.zip(cm).each{|v, (c, cp)| row[c] = ((v && cp) ? cp.call(v) : v)}
              row.delete(rn) if offset
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
