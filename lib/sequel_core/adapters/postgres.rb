require 'sequel_core/adapters/shared/postgres'

begin 
  require 'pg' 
rescue LoadError => e 
  begin 
    require 'postgres' 
    class PGconn
      unless method_defined?(:escape_string)
        if self.respond_to?(:escape)
          def escape_string(str)
            self.class.escape(str)
          end
        else
          def escape_string(obj)
            raise Sequel::Error, "string escaping not supported with this postgres driver.  Try using ruby-pg, ruby-postgres, or postgres-pr."
          end
        end
      end
      unless method_defined?(:escape_bytea)
        if self.respond_to?(:escape_bytea)
          def escape_bytea(obj)
            self.class.escape_bytea(obj)
          end
        else
          begin
            require 'postgres-pr/typeconv/conv'
            require 'postgres-pr/typeconv/bytea'
            extend Postgres::Conversion
            def escape_bytea(obj)
              self.class.encode_bytea(obj)
            end
            metaalias :unescape_bytea, :decode_bytea
          rescue
            def escape_bytea(obj)
              raise Sequel::Error, "bytea escaping not supported with this postgres driver.  Try using ruby-pg, ruby-postgres, or postgres-pr."
            end
            def self.unescape_bytea(obj)
              raise Sequel::Error, "bytea unescaping not supported with this postgres driver.  Try using ruby-pg, ruby-postgres, or postgres-pr."
            end
          end
        end
      end
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
    CONVERTED_EXCEPTIONS << PGError
    PG_TYPES = {
      16 => lambda{ |s| Postgres.string_to_bool(s) }, # boolean
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
    
    def self.string_to_bool(s)
      if(s.blank?)
        nil
      elsif(s.downcase == 't' || s.downcase == 'true')
        true
      else
        false
      end
    end
  
    class Adapter < ::PGconn
      include Sequel::Postgres::AdapterMethods
      self.translate_results = false if respond_to?(:translate_results=)
      
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
      
      def result_set_values(r, *vals)
        return if r.nil? || (r.ntuples == 0)
        case vals.length
        when 1
          r.getvalue(0, vals.first)
        else
          vals.collect{|col| r.getvalue(0, col)}
        end
      end
    end

    class Database < Sequel::Database
      include Sequel::Postgres::DatabaseMethods
      
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
      
      def dataset(opts = nil)
        Postgres::Dataset.new(self, opts)
      end
      
      def disconnect
        @pool.disconnect {|c| c.finish}
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
      
      private

      # PostgreSQL doesn't need the pool to convert exceptions, either.
      def connection_pool_default_options
        super.merge(:pool_convert_exceptions=>false)
      end
    end
  
    class Dataset < Sequel::Dataset
      include Sequel::Postgres::DatasetMethods

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

