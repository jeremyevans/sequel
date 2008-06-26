begin 
  require 'pg' 
rescue LoadError => e 
  begin 
    require 'postgres' 
    class PGconn
      metaalias :escape_string, :escape unless self.respond_to?(:escape_string)
      alias_method :finish, :close unless method_defined?(:finish) 
    end
    class PGresult 
      alias_method :nfields, :num_fields unless method_defined?(:nfields) 
      alias_method :ntuples, :num_tuples unless method_defined?(:ntuples) 
      alias_method :ftype, :type unless method_defined?(:ftype) 
      alias_method :fname, :fieldname unless method_defined?(:fname) 
      alias_method :cmd_tuples, :cmdtuples unless method_defined?(:cmd_tuples) 
    end 
  rescue LoadError 
    raise e 
  end 
end

module Sequel
  module Postgres
    class Adapter < ::PGconn
      # the pure-ruby postgres adapter does not have a quote method.
      TRUE = 'true'.freeze
      FALSE = 'false'.freeze
      NULL = 'NULL'.freeze
      
      def self.quote(obj)
        case obj
        when TrueClass
          TRUE
        when FalseClass
          FALSE
        when NilClass
          NULL
        when ::Sequel::SQL::Blob
          "'#{escape_bytea(obj)}'"
        else
          "'#{escape_string(obj.to_s)}'"
        end
      end
      
      def connected?
        status == Adapter::CONNECTION_OK
      end
      
      def execute(sql, &block)
        q = nil
        begin
          q = exec(sql)
        rescue PGError => e
          unless connected?
            reset
            q = exec(sql)
          else
            raise e
          end
        end
        begin
          block ? block[q] : q.cmd_tuples
        ensure
          q.clear
        end
      end
    
      attr_accessor :transaction_depth
      
      SELECT_CURRVAL = "SELECT currval('%s')".freeze
          
      def last_insert_id(table)
        @table_sequences ||= {}
        if !@table_sequences.include?(table)
          pkey_and_seq = pkey_and_sequence(table)
          if pkey_and_seq
            @table_sequences[table] = pkey_and_seq[1]
          end
        end
        if seq = @table_sequences[table]
          execute(SELECT_CURRVAL % seq) do |r|
            return r.getvalue(0,0).to_i unless r.nil? || (r.ntuples == 0)
          end
        end
        nil # primary key sequence not found
      end
          
      # Shamelessly appropriated from ActiveRecord's Postgresql adapter.
      SELECT_PK_AND_SERIAL_SEQUENCE = <<-end_sql
        SELECT attr.attname, name.nspname, seq.relname
        FROM pg_class seq, pg_attribute attr, pg_depend dep,
          pg_namespace name, pg_constraint cons
        WHERE seq.oid = dep.objid
          AND seq.relnamespace  = name.oid
          AND seq.relkind = 'S'
          AND attr.attrelid = dep.refobjid
          AND attr.attnum = dep.refobjsubid
          AND attr.attrelid = cons.conrelid
          AND attr.attnum = cons.conkey[1]
          AND cons.contype = 'p'
          AND dep.refobjid = '%s'::regclass
      end_sql
      
      SELECT_PK_AND_CUSTOM_SEQUENCE = <<-end_sql
        SELECT attr.attname,  
          CASE  
            WHEN split_part(def.adsrc, '''', 2) ~ '.' THEN  
              substr(split_part(def.adsrc, '''', 2),  
                     strpos(split_part(def.adsrc, '''', 2), '.')+1) 
            ELSE split_part(def.adsrc, '''', 2)  
          END
        FROM pg_class t
        JOIN pg_namespace  name ON (t.relnamespace = name.oid)
        JOIN pg_attribute  attr ON (t.oid = attrelid)
        JOIN pg_attrdef    def  ON (adrelid = attrelid AND adnum = attnum)
        JOIN pg_constraint cons ON (conrelid = adrelid AND adnum = conkey[1])
        WHERE t.oid = '%s'::regclass
          AND cons.contype = 'p'
          AND def.adsrc ~* 'nextval'
      end_sql
    
      SELECT_PK = <<-end_sql
        SELECT pg_attribute.attname
        FROM pg_class, pg_attribute, pg_index
        WHERE pg_class.oid = pg_attribute.attrelid AND
          pg_class.oid = pg_index.indrelid AND
          pg_index.indkey[0] = pg_attribute.attnum AND
          pg_index.indisprimary = 't' AND
          pg_class.relname = '%s'
      end_sql
      
      def pkey_and_sequence(table)
        execute(SELECT_PK_AND_SERIAL_SEQUENCE % table) do |r|
          return [r.getvalue(0,2), r.getvalue(0,2)] unless r.nil? || (r.ntuples == 0)
        end
    
        execute(SELECT_PK_AND_CUSTOM_SEQUENCE % table) do |r|
          return [r.getvalue(0,0), r.getvalue(0,1)] unless r.nil? || (r.ntuples == 0)
        end
      end
      
      def primary_key(table)
        execute(SELECT_PK % table) do |r|
          if (r.nil? || (r.ntuples == 0)) then
            return nil
          else
            r.getvalue(0,0)
          end
        end
      end
    
      def self.string_to_bool(s)
        if(s.blank?)
          nil
        elsif(s.downcase == 't' || s.downcase == 'true')
          true
        else
          false
        end
      end
    end

    PG_TYPES = {
      16 => lambda{ |s| Adapter.string_to_bool(s) }, # boolean
      17 => lambda{ |s| Adapter.unescape_bytea(s).to_blob }, # bytea
      20 => lambda{ |s| s.to_i }, # int8
      21 => lambda{ |s| s.to_i }, # int2
      22 => lambda{ |s| s.to_i }, # int2vector
      23 => lambda{ |s| s.to_i }, # int4
      26 => lambda{ |s| s.to_i }, # oid
      700 => lambda{ |s| s.to_f }, # float4
      701 => lambda{ |s| s.to_f }, # float8
      790 => lambda{ |s| s.to_d }, # money
      1082 => lambda{ |s| s.to_date }, # date
      1083 => lambda{ |s| s.to_time }, # time without time zone
      1114 => lambda{ |s| s.to_sequel_time }, # timestamp without time zone
      1184 => lambda{ |s| s.to_sequel_time }, # timestamp with time zone
      1186 => lambda{ |s| s.to_i }, # interval
      1266 => lambda{ |s| s.to_time }, # time with time zone
      1700 => lambda{ |s| s.to_d }, # numeric
    }

    if Adapter.respond_to?(:translate_results=)
      Adapter.translate_results = false
    end
    AUTO_TRANSLATE = false

    class Database < Sequel::Database
      set_adapter_scheme :postgres
    
      def connect
        conn = Adapter.connect(
          @opts[:host] || 'localhost',
          @opts[:port] || 5432,
          '', '',
          @opts[:database],
          @opts[:user],
          @opts[:password]
        )
        if encoding = @opts[:encoding] || @opts[:charset]
          conn.set_client_encoding(encoding)
        end
        conn
      end
      
      def disconnect
        @pool.disconnect {|c| c.finish}
      end
    
      def dataset(opts = nil)
        Postgres::Dataset.new(self, opts)
      end
    
      RELATION_QUERY = {:from => [:pg_class], :select => [:relname]}.freeze
      RELATION_FILTER = "(relkind = 'r') AND (relname !~ '^pg|sql')".freeze
      SYSTEM_TABLE_REGEXP = /^pg|sql/.freeze
    
      def tables
        dataset(RELATION_QUERY).filter(RELATION_FILTER).map {|r| r[:relname].to_sym}
      end
      
      def locks
        dataset.from("pg_class, pg_locks").
          select("pg_class.relname, pg_locks.*").
          filter("pg_class.relfilenode=pg_locks.relation")
      end
    
      def execute(sql, &block)
        begin
          log_info(sql)
          @pool.hold {|conn| conn.execute(sql, &block)}
        rescue => e
          log_info(e.message)
          raise convert_pgerror(e)
        end
      end
    
      def primary_key_for_table(conn, table)
        @primary_keys ||= {}
        @primary_keys[table] ||= conn.primary_key(table)
      end
      
      RE_CURRVAL_ERROR = /currval of sequence "(.*)" is not yet defined in this session/.freeze
      
      def insert_result(conn, table, values)
        begin
          result = conn.last_insert_id(table)
          return result if result
        rescue PGError => e
          raise(Error, e.message) unless RE_CURRVAL_ERROR.match(e.message)
        end
        
        case values
        when Hash
          values[primary_key_for_table(conn, table)]
        when Array
          values.first
        else
          nil
        end
      end
      
      def server_version
        return @server_version if @server_version
        @server_version = pool.hold do |conn|
          if conn.respond_to?(:server_version)
            begin
              conn.server_version
            rescue StandardError
              nil
            end
          end
        end
        unless @server_version
          m = /PostgreSQL (\d+)\.(\d+)\.(\d+)/.match(get(:version[]))
          @server_version = (m[1].to_i * 10000) + (m[2].to_i * 100) + m[3].to_i
        end
        @server_version
      end
      
      def execute_insert(sql, table, values)
        begin 
          log_info(sql)
          @pool.hold do |conn|
            conn.execute(sql)
            insert_result(conn, table, values)
          end
        rescue => e
          log_info(e.message)
          raise convert_pgerror(e)
        end
      end
    
      SQL_BEGIN = 'BEGIN'.freeze
      SQL_SAVEPOINT = 'SAVEPOINT autopoint_%d'.freeze
      SQL_COMMIT = 'COMMIT'.freeze
      SQL_ROLLBACK_TO_SAVEPOINT = 'ROLLBACK TO SAVEPOINT autopoint_%d'.freeze
      SQL_ROLLBACK = 'ROLLBACK'.freeze
      SQL_RELEASE_SAVEPOINT = 'RELEASE SAVEPOINT autopoint_%d'.freeze
  
      def transaction
        @pool.hold do |conn|
          conn.transaction_depth = 0 if conn.transaction_depth.nil?
          if conn.transaction_depth > 0
            log_info(SQL_SAVEPOINT % conn.transaction_depth)
            conn.execute(SQL_SAVEPOINT % conn.transaction_depth)
          else
            log_info(SQL_BEGIN)
            conn.execute(SQL_BEGIN)
          end
          begin
            conn.transaction_depth += 1
            yield conn
          rescue ::Exception => e
            if conn.transaction_depth > 1
              log_info(SQL_ROLLBACK_TO_SAVEPOINT % [conn.transaction_depth - 1])
              conn.execute(SQL_ROLLBACK_TO_SAVEPOINT % [conn.transaction_depth - 1])
            else
              log_info(SQL_ROLLBACK)
              conn.execute(SQL_ROLLBACK) rescue nil
            end
            raise convert_pgerror(e) unless Error::Rollback === e
          ensure
            unless e
              begin
                if conn.transaction_depth < 2
                  log_info(SQL_COMMIT)
                  conn.execute(SQL_COMMIT)
                else
                  log_info(SQL_RELEASE_SAVEPOINT % [conn.transaction_depth - 1])
                  conn.execute(SQL_RELEASE_SAVEPOINT % [conn.transaction_depth - 1])
                end
              rescue => e
                log_info(e.message)
                raise convert_pgerror(e)
              end
            end
            conn.transaction_depth -= 1
          end
        end
      end

      def serial_primary_key_options
        {:primary_key => true, :type => :serial}
      end

      def index_definition_sql(table_name, index)
        index_name = index[:name] || default_index_name(table_name, index[:columns])
        expr = literal(Array(index[:columns]))
        unique = "UNIQUE " if index[:unique]
        index_type = index[:type]
        filter = index[:where] || index[:filter]
        filter = " WHERE #{filter_expr(filter)}" if filter
        case index_type
        when :full_text
          lang = index[:language] ? "#{literal(index[:language])}, " : ""
          cols = index[:columns].map {|c| literal(c)}.join(" || ")
          expr = "(to_tsvector(#{lang}#{cols}))"
          index_type = :gin
        when :spatial
          index_type = :gist
        end
        "CREATE #{unique}INDEX #{index_name} ON #{table_name} #{"USING #{index_type} " if index_type}#{expr}#{filter}"
      end
    
      def drop_table_sql(name)
        "DROP TABLE #{name} CASCADE"
      end

      private
      # If the given exception is a PGError, return a Sequel::Error with the same message, otherwise
      # just return the given exception
      def convert_pgerror(e)
        PGError === e ? Error.new(e.message) : e
      end

      # PostgreSQL currently can always reuse connections.  It doesn't need the pool to convert exceptions, either.
      def connection_pool_default_options
        super.merge(:pool_convert_exceptions=>false)
      end

      def schema_ds_filter(table_name, opts)
        filt = super
        # Restrict it to the given or public schema, unless specifically requesting :schema = nil
        filt = SQL::BooleanExpression.new(:AND, filt, {:c__table_schema=>opts[:schema] || 'public'}) if opts[:schema] || !opts.include?(:schema)
        filt
      end
    end
  
    class Dataset < Sequel::Dataset
      def quoted_identifier(c)
        "\"#{c}\""
      end
      
      PG_TIMESTAMP_FORMAT = "TIMESTAMP '%Y-%m-%d %H:%M:%S".freeze

      def literal(v)
        case v
        when LiteralString
          v
        when ::Sequel::SQL::Blob, String, TrueClass, FalseClass
          Adapter.quote(v)
        when Time
          "#{v.strftime(PG_TIMESTAMP_FORMAT)}.#{sprintf("%06d",v.usec)}'"
        when DateTime
          "#{v.strftime(PG_TIMESTAMP_FORMAT)}.#{sprintf("%06d", (v.sec_fraction * 86400000000).to_i)}'"
        else
          super
        end
      end
    
      def match_expr(l, r)
        case r
        when Regexp
          r.casefold? ? \
            "(#{literal(l)} ~* #{literal(r.source)})" :
            "(#{literal(l)} ~ #{literal(r.source)})"
        else
          super
        end
      end
      
      def full_text_search(cols, terms, opts = {})
        lang = opts[:language] ? "#{literal(opts[:language])}, " : ""
        cols = cols.is_a?(Array) ? cols.map {|c| literal(c)}.join(" || ") : literal(cols)
        terms = terms.is_a?(Array) ? literal(terms.join(" | ")) : literal(terms)
        filter("to_tsvector(#{lang}#{cols}) @@ to_tsquery(#{lang}#{terms})")
      end

      FOR_UPDATE = ' FOR UPDATE'.freeze
      FOR_SHARE = ' FOR SHARE'.freeze
    
      def select_sql(opts = nil)
        row_lock_mode = opts ? opts[:lock] : @opts[:lock]
        sql = super
        case row_lock_mode
        when :update
          sql << FOR_UPDATE
        when :share
          sql << FOR_SHARE
        end
        sql
      end
    
      def for_update
        clone(:lock => :update)
      end
    
      def for_share
        clone(:lock => :share)
      end
    
      EXPLAIN = 'EXPLAIN '.freeze
      EXPLAIN_ANALYZE = 'EXPLAIN ANALYZE '.freeze
      QUERY_PLAN = 'QUERY PLAN'.to_sym
    
      def explain(opts = nil)
        analysis = []
        fetch_rows(EXPLAIN + select_sql(opts)) do |r|
          analysis << r[QUERY_PLAN]
        end
        analysis.join("\r\n")
      end
      
      def analyze(opts = nil)
        analysis = []
        fetch_rows(EXPLAIN_ANALYZE + select_sql(opts)) do |r|
          analysis << r[QUERY_PLAN]
        end
        analysis.join("\r\n")
      end
    
      LOCK = 'LOCK TABLE %s IN %s MODE'.freeze
    
      ACCESS_SHARE = 'ACCESS SHARE'.freeze
      ROW_SHARE = 'ROW SHARE'.freeze
      ROW_EXCLUSIVE = 'ROW EXCLUSIVE'.freeze
      SHARE_UPDATE_EXCLUSIVE = 'SHARE UPDATE EXCLUSIVE'.freeze
      SHARE = 'SHARE'.freeze
      SHARE_ROW_EXCLUSIVE = 'SHARE ROW EXCLUSIVE'.freeze
      EXCLUSIVE = 'EXCLUSIVE'.freeze
      ACCESS_EXCLUSIVE = 'ACCESS EXCLUSIVE'.freeze
    
      # Locks the table with the specified mode.
      def lock(mode, &block)
        sql = LOCK % [source_list(@opts[:from]), mode]
        @db.synchronize do
          if block # perform locking inside a transaction and yield to block
            @db.transaction {@db.execute(sql); yield}
          else
            @db.execute(sql) # lock without a transaction
            self
          end
        end
      end
      
      def multi_insert_sql(columns, values)
        return super if @db.server_version < 80200
        
        # postgresql 8.2 introduces support for multi-row insert
        columns = column_list(columns)
        values = values.map {|r| literal(Array(r))}.join(COMMA_SEPARATOR)
        ["INSERT INTO #{source_list(@opts[:from])} (#{columns}) VALUES #{values}"]
      end

      def insert(*values)
        @db.execute_insert(insert_sql(*values), source_list(@opts[:from]),
          values.size == 1 ? values.first : values)
      end
    
      def update(*args, &block)
        @db.execute(update_sql(*args, &block))
      end
    
      def delete(opts = nil)
        @db.execute(delete_sql(opts))
      end

      def fetch_rows(sql, &block)
        @columns = []
        @db.execute(sql) do |res|
          (0...res.ntuples).each do |recnum|
            converted_rec = {}
            (0...res.nfields).each do |fieldnum|
              fieldsym = res.fname(fieldnum).to_sym
              @columns << fieldsym
              converted_rec[fieldsym] = if value = res.getvalue(recnum,fieldnum)
                (PG_TYPES[res.ftype(fieldnum)] || lambda{|s| s.to_s}).call(value)
              else
                value
              end
            end
            yield converted_rec
          end
        end
      end
    end
  end
end

