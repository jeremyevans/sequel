require 'ibm_db'
Sequel.require 'adapters/shared/db2'

module Sequel

  module IBMDB
    @convert_smallint_to_bool = true

    class << self
      # Whether to convert smallint values to bool, true by default.
      # Can also be overridden per dataset.
      attr_accessor :convert_smallint_to_bool
    end

    tt = Class.new do
      def boolean(s) !s.to_i.zero? end
      def int(s) s.to_i end
    end.new

    # Hash holding type translation methods, used by Dataset#fetch_rows.
    DB2_TYPES = {
      :boolean => tt.method(:boolean),
      :int => tt.method(:int),
      :blob => ::Sequel::SQL::Blob.method(:new),
      :time => ::Sequel.method(:string_to_time),
      :date => ::Sequel.method(:string_to_date)
    }
    DB2_TYPES[:clob] = DB2_TYPES[:blob]

    # Wraps an underlying connection to DB2 using IBM_DB.
    class Connection
      # A hash with prepared statement name symbol keys, where each value is
      # a two element array with an sql string and cached Statement value.
      attr_accessor :prepared_statements

      # Error class for exceptions raised by the connection.
      class Error < StandardError
      end

      # Create the underlying IBM_DB connection.
      def initialize(connection_string)
        @conn = IBM_DB.connect(connection_string, '', '')
        self.autocommit = true
        @prepared_statements = {}
      end

      # Check whether the connection is in autocommit state or not.
      def autocommit
        IBM_DB.autocommit(@conn) == 1
      end

      # Turn autocommit on or off for the connection.
      def autocommit=(value)
        IBM_DB.autocommit(@conn, value ? IBM_DB::SQL_AUTOCOMMIT_ON : IBM_DB::SQL_AUTOCOMMIT_OFF)
      end

      # Close the connection, disconnecting from DB2.
      def close
        IBM_DB.close(@conn)
      end

      # Commit the currently outstanding transaction on this connection.
      def commit
        IBM_DB.commit(@conn)
      end

      # Return the related error message for the connection.
      def error_msg
        IBM_DB.getErrormsg(@conn, IBM_DB::DB_CONN)
      end

      # Execute the given SQL on the database, and return a Statement instance
      # holding the results.
      def execute(sql)
        stmt = IBM_DB.exec(@conn, sql)
        raise Error, error_msg unless stmt
        Statement.new(stmt)
      end

      # Execute the related prepared statement on the database with the given
      # arguments.
      def execute_prepared(ps_name, *values)
        stmt = @prepared_statements[ps_name].last
        res = stmt.execute(*values)
        unless res
          raise Error, "Error executing statement #{ps_name}: #{error_msg}"
        end
        stmt
      end

      # Prepare a statement with the given +sql+ on the database, and
      # cache the prepared statement value by name.
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

      # Rollback the currently outstanding transaction on this connection.
      def rollback
        IBM_DB.rollback(@conn)
      end
    end

    # Wraps results returned by queries on IBM_DB.
    class Statement
      # Hold the given statement.
      def initialize(stmt)
        @stmt = stmt
      end

      # Return the number of rows affected.
      def affected
        IBM_DB.num_rows(@stmt)
      end

      # If this statement is a prepared statement, execute it on the database
      # with the given values.
      def execute(*values)
        IBM_DB.execute(@stmt, values)
      end

      # Return the results of a query as an array of values.
      def fetch_array
        IBM_DB.fetch_array(@stmt) if @stmt
      end

      # Return the field name at the given column in the result set.
      def field_name(ind)
        IBM_DB.field_name(@stmt, ind)
      end

      # Return the field type for the given field name in the result set.
      def field_type(key)
        IBM_DB.field_type(@stmt, key)
      end

      # Return the field precision for the given field name in the result set.
      def field_precision(key)
        IBM_DB.field_precision(@stmt, key)
      end

      # Free the memory related to this result set.
      def free
        IBM_DB.free_result(@stmt)
      end

      # Return the number of fields in the result set.
      def num_fields
        IBM_DB.num_fields(@stmt)
      end
    end

    class Database < Sequel::Database
      include Sequel::DB2::DatabaseMethods

      set_adapter_scheme :ibmdb

      # Hash of connection procs for converting
      attr_reader :conversion_procs

      def initialize(opts={})
        super
        @conversion_procs = DB2_TYPES.dup
        @conversion_procs[:timestamp] = method(:to_application_timestamp)
      end

      # REORG the related table whenever it is altered.  This is not always
      # required, but it is necessary for compatibilty with other Sequel
      # code in many cases.
      def alter_table(name, generator=nil)
        res = super
        reorg(name)
        res
      end

      # Create a new connection object for the given server.
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

      # Execute the given SQL on the database.
      def execute(sql, opts={}, &block)
        if sql.is_a?(Symbol)
          execute_prepared_statement(sql, opts, &block)
        else
          synchronize(opts[:server]){|c| _execute(c, sql, opts, &block)}
        end
      rescue Connection::Error => e
        raise_error(e)
      end

      # Execute the given SQL on the database, returning the last inserted
      # identity value.
      def execute_insert(sql, opts={})
        synchronize(opts[:server]) do |c|
          if sql.is_a?(Symbol)
            execute_prepared_statement(sql, opts)
          else
            _execute(c, sql, opts)
          end
          _execute(c, "SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1", opts){|stmt| i = stmt.fetch_array.first.to_i; stmt.free; i}
        end
      rescue Connection::Error => e
        raise_error(e)
      end

      # Execute a prepared statement named by name on the database.
      def execute_prepared_statement(ps_name, opts)
        args = opts[:arguments]
        ps = prepared_statement(ps_name)
        sql = ps.prepared_sql
        synchronize(opts[:server]) do |conn|
          unless conn.prepared_statements.fetch(ps_name, []).first == sql
            log_yield("PREPARE #{ps_name}: #{sql}"){conn.prepare(sql, ps_name)}
          end
          args = args.map{|v| v.nil? ? nil : prepared_statement_arg(v)}
          log_sql = "EXECUTE #{ps_name}"
          if ps.log_sql
            log_sql << " ("
            log_sql << sql
            log_sql << ")"
          end
          stmt = log_yield(log_sql, args){conn.execute_prepared(ps_name, *args)}
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

      # Convert smallint type to boolean if convert_smallint_to_bool is true
      def schema_column_type(db_type)
        if Sequel::IBMDB.convert_smallint_to_bool && db_type =~ /smallint/i
          :boolean
        else
          super
        end
      end

      # On DB2, a table might need to be REORGed if you are testing existence
      # of it.  This REORGs automatically if the database raises a specific
      # error that indicates it should be REORGed.
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

      # Execute the given SQL on the database.
      def _execute(conn, sql, opts)
        stmt = log_yield(sql){conn.execute(sql)}
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

      # IBM_DB uses an autocommit setting instead of sending SQL queries.
      # So starting a transaction just turns autocommit off.
      def begin_transaction(conn, opts={})
        log_yield(TRANSACTION_BEGIN){conn.autocommit = false}
      end

      # This commits transaction in progress on the
      # connection and sets autocommit back on.
      def commit_transaction(conn, opts={})
        log_yield(TRANSACTION_COMMIT){conn.commit}
      end

      # Close the given connection.
      def disconnect_connection(conn)
        conn.close
      end

      # Don't convert smallint to boolean for the metadata
      # dataset, since the DB2 metadata does not use
      # boolean columns, and some smallint columns are
      # accidently treated as booleans.
      def metadata_dataset
        ds = super
        ds.convert_smallint_to_bool = false
        ds
      end

      # Format Numeric, Date, and Time types specially for use
      # as IBM_DB prepared statements argument vlaues.
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

      # Set autocommit back on
      def remove_transaction(conn, committed)
        conn.autocommit = true
      ensure
        super
      end

      # This rolls back the transaction in progress on the
      # connection and sets autocommit back on.
      def rollback_transaction(conn, opts={})
        log_yield(TRANSACTION_ROLLBACK){conn.rollback}
      end
    end

    class Dataset < Sequel::Dataset
      include Sequel::DB2::DatasetMethods

      Database::DatasetClass = self

      module CallableStatementMethods
        # Extend given dataset with this module so subselects inside subselects in
        # prepared statements work.
        def subselect_sql_append(sql, ds)
          ps = ds.to_prepared_statement(:select).clone(:append_sql=>sql)
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
        # Execute the prepared statement with arguments instead of the given SQL.
        def execute(sql, opts={}, &block)
          super(prepared_statement_name, {:arguments=>bind_arguments}.merge(opts), &block)
        end

        # Execute the prepared statment with arguments instead of the given SQL.
        def execute_dui(sql, opts={}, &block)
          super(prepared_statement_name, {:arguments=>bind_arguments}.merge(opts), &block)
        end

        # Execute the prepared statement with arguments instead of the given SQL.
        def execute_insert(sql, opts={}, &block)
          super(prepared_statement_name, {:arguments=>bind_arguments}.merge(opts), &block)
        end

      end

      # Emulate support of bind arguments in called statements.
      def call(type, bind_arguments={}, *values, &block)
        ps = to_prepared_statement(type, values)
        ps.extend(CallableStatementMethods)
        ps.call(bind_arguments, &block)
      end

      # Whether to convert smallint to boolean arguments for this dataset.
      # Defaults to the IBMDB module setting.
      def convert_smallint_to_bool
        defined?(@convert_smallint_to_bool) ? @convert_smallint_to_bool : (@convert_smallint_to_bool = IBMDB.convert_smallint_to_bool)
      end

      # Override the default IBMDB.convert_smallint_to_bool setting for this dataset.
      attr_writer :convert_smallint_to_bool

      # Fetch the rows from the database and yield plain hashes.
      def fetch_rows(sql)
        execute(sql) do |stmt|
          offset = @opts[:offset]
          columns = []
          convert = convert_smallint_to_bool
          cps = db.conversion_procs
          stmt.num_fields.times do |i|
            k = stmt.field_name i
            key = output_identifier(k)
            type = stmt.field_type(k).downcase.to_sym
            # decide if it is a smallint from precision
            type = :boolean  if type ==:int && convert && stmt.field_precision(k) < 8
            columns << [key, cps[type]]
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
          db.set_prepared_statement(name, ps)
        end
        ps
      end
    end
  end
end
