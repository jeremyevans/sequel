# frozen-string-literal: true

require 'sqlite3'
Sequel.require 'adapters/shared/sqlite'

module Sequel
  # Top level module for holding all SQLite-related modules and classes
  # for Sequel.
  module SQLite
    TYPE_TRANSLATOR = tt = Class.new do
      FALSE_VALUES = (%w'0 false f no n' + [0]).freeze

      def blob(s)
        Sequel::SQL::Blob.new(s.to_s)
      end

      def boolean(s)
        s = s.downcase if s.is_a?(String)
        !FALSE_VALUES.include?(s)
      end

      def date(s)
        case s
        when String
          Sequel.string_to_date(s)
        when Integer
          Date.jd(s)
        when Float
          Date.jd(s.to_i)
        else
          raise Sequel::Error, "unhandled type when converting to date: #{s.inspect} (#{s.class.inspect})"
        end
      end

      def integer(s)
        s.to_i
      end

      def float(s)
        s.to_f
      end

      def numeric(s)
        s = s.to_s unless s.is_a?(String)
        ::BigDecimal.new(s) rescue s
      end

      def time(s)
        case s
        when String
          Sequel.string_to_time(s)
        when Integer
          Sequel::SQLTime.create(s/3600, (s % 3600)/60, s % 60)
        when Float
          s, f = s.divmod(1)
          Sequel::SQLTime.create(s/3600, (s % 3600)/60, s % 60, (f*1000000).round)
        else
          raise Sequel::Error, "unhandled type when converting to date: #{s.inspect} (#{s.class.inspect})"
        end
      end

    end.new

    # Hash with string keys and callable values for converting SQLite types.
    SQLITE_TYPES = {}
    {
      %w'date' => tt.method(:date),
      %w'time' => tt.method(:time),
      %w'bit bool boolean' => tt.method(:boolean),
      %w'integer smallint mediumint int bigint' => tt.method(:integer),
      %w'numeric decimal money' => tt.method(:numeric),
      %w'float double real dec fixed' + ['double precision'] => tt.method(:float),
      %w'blob' => tt.method(:blob)
    }.each do |k,v|
      k.each{|n| SQLITE_TYPES[n] = v}
    end
    # SQLITE_TYPES.freeze # SEQUEL5
    
    # Database class for SQLite databases used with Sequel and the
    # ruby-sqlite3 driver.
    class Database < Sequel::Database
      include ::Sequel::SQLite::DatabaseMethods
      
      set_adapter_scheme :sqlite
      
      # Mimic the file:// uri, by having 2 preceding slashes specify a relative
      # path, and 3 preceding slashes specify an absolute path.
      def self.uri_to_options(uri) # :nodoc:
        { :database => (uri.host.nil? && uri.path == '/') ? nil : "#{uri.host}#{uri.path}" }
      end
      
      private_class_method :uri_to_options

      # The conversion procs to use for this database
      attr_reader :conversion_procs

      # Connect to the database. Since SQLite is a file based database,
      # available options are limited:
      #
      # :database :: database name (filename or ':memory:' or file: URI)
      # :readonly :: open database in read-only mode; useful for reading
      #              static data that you do not want to modify
      # :timeout :: how long to wait for the database to be available if it
      #             is locked, given in milliseconds (default is 5000)
      # :extensions :: filename or array of filenames of SQLITE3 extension 
      #                files (shared objects) to load
      # 
      def connect(server)
        opts = server_opts(server)
        opts[:database] = ':memory:' if blank_object?(opts[:database])
        sqlite3_opts = {}
        sqlite3_opts[:readonly] = typecast_value_boolean(opts[:readonly]) if opts.has_key?(:readonly)
        db = ::SQLite3::Database.new(opts[:database].to_s, sqlite3_opts)
        db.busy_timeout(opts.fetch(:timeout, 5000))
        
        if opts.has_key?(:extensions)
          ext = opts[:extensions]
          ext = [ ext ] if ext.kind_of?(String) 
          db.enable_load_extension(1)
          ext.each do |file|
            log_connection_yield(file, db) { db.load_extension(file) }
          end
          db.enable_load_extension(0)
        end

        connection_pragmas.each{|s| log_connection_yield(s, db){db.execute_batch(s)}}
        
        class << db
          attr_reader :prepared_statements
        end
        db.instance_variable_set(:@prepared_statements, {})
        
        db
      end

      # Disconnect given connections from the database.
      def disconnect_connection(c)
        c.prepared_statements.each_value{|v| v.first.close}
        c.close
      end
      
      # Run the given SQL with the given arguments and yield each row.
      def execute(sql, opts=OPTS, &block)
        _execute(:select, sql, opts, &block)
      end

      # Run the given SQL with the given arguments and return the number of changed rows.
      def execute_dui(sql, opts=OPTS)
        _execute(:update, sql, opts)
      end
      
      # Drop any prepared statements on the connection when executing DDL.  This is because
      # prepared statements lock the table in such a way that you can't drop or alter the
      # table while a prepared statement that references it still exists.
      def execute_ddl(sql, opts=OPTS)
        synchronize(opts[:server]) do |conn|
          conn.prepared_statements.values.each{|cps, s| cps.close}
          conn.prepared_statements.clear
          super
        end
      end
      
      # Run the given SQL with the given arguments and return the last inserted row id.
      def execute_insert(sql, opts=OPTS)
        _execute(:insert, sql, opts)
      end
      
      def freeze
        @conversion_procs.freeze
        super
      end

      # Handle Integer and Float arguments, since SQLite can store timestamps as integers and floats.
      def to_application_timestamp(s)
        case s
        when String
          super
        when Integer
          super(Time.at(s).to_s)
        when Float
          super(DateTime.jd(s).to_s)
        else
          raise Sequel::Error, "unhandled type when converting to : #{s.inspect} (#{s.class.inspect})"
        end
      end

      private
      
      def adapter_initialize
        @conversion_procs = SQLITE_TYPES.dup
        @conversion_procs['datetime'] = @conversion_procs['timestamp'] = method(:to_application_timestamp)
        set_integer_booleans
      end
      
      # Yield an available connection.  Rescue
      # any SQLite3::Exceptions and turn them into DatabaseErrors.
      def _execute(type, sql, opts, &block)
        begin
          synchronize(opts[:server]) do |conn|
            return execute_prepared_statement(conn, type, sql, opts, &block) if sql.is_a?(Symbol)
            log_args = opts[:arguments]
            args = {}
            opts.fetch(:arguments, {}).each{|k, v| args[k] = prepared_statement_argument(v)}
            case type
            when :select
              log_connection_yield(sql, conn, log_args){conn.query(sql, args, &block)}
            when :insert
              log_connection_yield(sql, conn, log_args){conn.execute(sql, args)}
              conn.last_insert_row_id
            when :update
              log_connection_yield(sql, conn, log_args){conn.execute_batch(sql, args)}
              conn.changes
            end
          end
        rescue SQLite3::Exception => e
          raise_error(e)
        end
      end
      
      # The SQLite adapter does not need the pool to convert exceptions.
      # Also, force the max connections to 1 if a memory database is being
      # used, as otherwise each connection gets a separate database.
      def connection_pool_default_options
        o = super.dup
        # Default to only a single connection if a memory database is used,
        # because otherwise each connection will get a separate database
        o[:max_connections] = 1 if @opts[:database] == ':memory:' || blank_object?(@opts[:database])
        o
      end
      
      def prepared_statement_argument(arg)
        case arg
        when Date, DateTime, Time
          literal(arg)[1...-1]
        when SQL::Blob
          arg.to_blob
        when true, false
          if integer_booleans
            arg ? 1 : 0
          else
            literal(arg)[1...-1]
          end
        else
          arg
        end
      end

      # Execute a prepared statement on the database using the given name.
      def execute_prepared_statement(conn, type, name, opts, &block)
        ps = prepared_statement(name)
        sql = ps.prepared_sql
        args = opts[:arguments]
        ps_args = {}
        args.each{|k, v| ps_args[k] = prepared_statement_argument(v)}
        if cpsa = conn.prepared_statements[name]
          cps, cps_sql = cpsa
          if cps_sql != sql
            cps.close
            cps = nil
          end
        end
        unless cps
          cps = log_connection_yield("PREPARE #{name}: #{sql}", conn){conn.prepare(sql)}
          conn.prepared_statements[name] = [cps, sql]
        end
        log_sql = String.new
        log_sql << "EXECUTE #{name}"
        if ps.log_sql
          log_sql << " ("
          log_sql << sql
          log_sql << ")"
        end
        if block
          log_connection_yield(log_sql, conn, args){cps.execute(ps_args, &block)}
        else
          log_connection_yield(log_sql, conn, args){cps.execute!(ps_args){|r|}}
          case type
          when :insert
            conn.last_insert_row_id
          when :update
            conn.changes
          end
        end
      end
      
      # SQLite3 raises ArgumentError in addition to SQLite3::Exception in
      # some cases, such as operations on a closed database.
      def database_error_classes
        [SQLite3::Exception, ArgumentError]
      end

      def dataset_class_default
        Dataset
      end

      # Support SQLite exception codes if ruby-sqlite3 supports them.
      # This is disabled by default because ruby-sqlite3 doesn't currently
      # support them (returning nil), and even if it did, it doesn't support
      # extended error codes, which would lead to worse behavior.
      #def sqlite_error_code(exception)
      #  exception.code if exception.respond_to?(:code)
      #end
    end
    
    # Dataset class for SQLite datasets that use the ruby-sqlite3 driver.
    class Dataset < Sequel::Dataset
      include ::Sequel::SQLite::DatasetMethods

      Database::DatasetClass = self
      Sequel::Deprecation.deprecate_constant(Database, :DatasetClass)
      
      PREPARED_ARG_PLACEHOLDER = ':'.freeze
      Sequel::Deprecation.deprecate_constant(self, :PREPARED_ARG_PLACEHOLDER)
      
      # SQLite already supports named bind arguments, so use directly.
      module ArgumentMapper
        include Sequel::Dataset::ArgumentMapper
        
        protected
        
        # Return a hash with the same values as the given hash,
        # but with the keys converted to strings.
        def map_to_prepared_args(hash)
          args = {}
          hash.each{|k,v| args[k.to_s.gsub('.', '__')] = v}
          args
        end
        
        private
        
        # SQLite uses a : before the name of the argument for named
        # arguments.
        def prepared_arg(k)
          LiteralString.new("#{prepared_arg_placeholder}#{k.to_s.gsub('.', '__')}")
        end

        # Always assume a prepared argument.
        def prepared_arg?(k)
          true
        end
      end
      
      BindArgumentMethods = prepared_statements_module(:bind, ArgumentMapper)
      PreparedStatementMethods = prepared_statements_module(:prepare, BindArgumentMethods)

      # Yield a hash for each row in the dataset.
      def fetch_rows(sql)
        execute(sql) do |result|
          i = -1
          cps = db.conversion_procs
          type_procs = result.types.map{|t| cps[base_type_name(t)]}
          cols = result.columns.map{|c| i+=1; [output_identifier(c), i, type_procs[i]]}
          self.columns = cols.map(&:first)
          result.each do |values|
            row = {}
            cols.each do |name,id,type_proc|
              v = values[id]
              if type_proc && v
                v = type_proc.call(v)
              end
              row[name] = v
            end
            yield row
          end
        end
      end
      
      private
      
      # The base type name for a given type, without any parenthetical part.
      def base_type_name(t)
        (t =~ /^(.*?)\(/ ? $1 : t).downcase if t
      end

      # Quote the string using the adapter class method.
      def literal_string_append(sql, v)
        sql << "'" << ::SQLite3::Database.quote(v) << "'"
      end

      def bound_variable_modules
        [BindArgumentMethods]
      end

      def prepared_statement_modules
        [PreparedStatementMethods]
      end

      # SQLite uses a : before the name of the argument as a placeholder.
      def prepared_arg_placeholder
        ':'
      end
    end
  end
end
