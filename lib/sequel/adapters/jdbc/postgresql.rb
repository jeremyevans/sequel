# frozen-string-literal: true

Sequel::JDBC.load_driver('org.postgresql.Driver', :Postgres)
Sequel.require 'adapters/shared/postgres'

module Sequel
  Postgres::CONVERTED_EXCEPTIONS << NativeException
  
  module JDBC
    Sequel.synchronize do
      DATABASE_SETUP[:postgresql] = proc do |db|
        db.dataset_class = Sequel::JDBC::Postgres::Dataset
        db.extend(Sequel::JDBC::Postgres::DatabaseMethods)
        org.postgresql.Driver
      end
    end

    class TypeConvertor
      # Return PostgreSQL array types as ruby Arrays instead of
      # JDBC PostgreSQL driver-specific array type. Only used if the
      # database does not have a conversion proc for the type.
      def RubyPGArray(r, i)
        if v = r.getArray(i)
          v.array.to_ary
        end
      end 

      # Return PostgreSQL hstore types as ruby Hashes instead of
      # Java HashMaps.  Only used if the database does not have a
      # conversion proc for the type.
      def RubyPGHstore(r, i)
        if v = r.getObject(i)
          v.to_hash
        end
      end 
    end

    # Adapter, Database, and Dataset support for accessing a PostgreSQL
    # database via JDBC.
    module Postgres
      # Methods to add to Database instances that access PostgreSQL via
      # JDBC.
      module DatabaseMethods
        include Sequel::Postgres::DatabaseMethods

        # Add the primary_keys and primary_key_sequences instance variables,
        # so we can get the correct return values for inserted rows.
        def self.extended(db)
          super
          db.send(:initialize_postgres_adapter)
        end

        # See Sequel::Postgres::Adapter#copy_into
        def copy_into(table, opts=OPTS)
          data = opts[:data]
          data = Array(data) if data.is_a?(String)

          if block_given? && data
            raise Error, "Cannot provide both a :data option and a block to copy_into"
          elsif !block_given? && !data
            raise Error, "Must provide either a :data option or a block to copy_into"
          end

          synchronize(opts) do |conn|
            begin
              copy_manager = org.postgresql.copy.CopyManager.new(conn)
              copier = copy_manager.copy_in(copy_into_sql(table, opts))
              if block_given?
                while buf = yield
                  copier.writeToCopy(buf.to_java_bytes, 0, buf.length)
                end
              else
                data.each { |d| copier.writeToCopy(d.to_java_bytes, 0, d.length) }
              end
            rescue Exception => e
              copier.cancelCopy
              raise
            ensure
              unless e
                begin
                  copier.endCopy
                rescue NativeException => e2
                  raise_error(e2)
                end
              end
            end
          end
        end
        
        # See Sequel::Postgres::Adapter#copy_table
        def copy_table(table, opts=OPTS)
          synchronize(opts[:server]) do |conn|
            copy_manager = org.postgresql.copy.CopyManager.new(conn)
            copier = copy_manager.copy_out(copy_table_sql(table, opts))
            begin
              if block_given?
                while buf = copier.readFromCopy
                  yield(String.from_java_bytes(buf))
                end
                nil
              else
                b = String.new
                while buf = copier.readFromCopy
                  b << String.from_java_bytes(buf)
                end
                b
              end
            ensure
              raise DatabaseDisconnectError, "disconnecting as a partial COPY may leave the connection in an unusable state" if buf
            end
          end
        end

        def oid_convertor_proc(oid)
          if (conv = Sequel.synchronize{@oid_convertor_map[oid]}).nil?
            conv = if pr = conversion_procs[oid]
              lambda do |r, i|
                if v = r.getString(i)
                  pr.call(v)
                end
              end
            else
              false
            end
             Sequel.synchronize{@oid_convertor_map[oid] = conv}
          end
          conv
        end

        private
        
        # Clear oid convertor map cache when conversion procs are updated.
        def conversion_procs_updated
          super
          Sequel.synchronize{@oid_convertor_map = {}}
        end

        def disconnect_error?(exception, opts)
          super || exception.message =~ /\A(This connection has been closed\.|FATAL: terminating connection due to administrator command|An I\/O error occurred while sending to the backend\.)\z/
        end

        # For PostgreSQL-specific types, return the string that should be used
        # as the PGObject value. Returns nil by default, loading pg_* extensions
        # will override this to add support for specific types.
        def bound_variable_arg(arg, conn)
          nil
        end

        # If the given argument is a recognized PostgreSQL-specific type, create
        # a PGObject instance with unknown type and the bound argument string value,
        # and set that as the prepared statement argument.
        def set_ps_arg(cps, arg, i)
          if v = bound_variable_arg(arg, nil)
            obj = org.postgresql.util.PGobject.new
            obj.setType("unknown")
            obj.setValue(v)
            cps.setObject(i, obj)
          else
            super
          end
        end

        # Use setNull for nil arguments as the default behavior of setString
        # with nil doesn't appear to work correctly on PostgreSQL.
        def set_ps_arg_nil(cps, i)
          cps.setNull(i, JavaSQL::Types::NULL)
        end

        # Execute the connection configuration SQL queries on the connection.
        def setup_connection(conn)
          conn = super(conn)
          statement(conn) do |stmt|
            connection_configuration_sqls.each{|sql| log_connection_yield(sql, conn){stmt.execute(sql)}}
          end
          conn
        end

        def setup_type_convertor_map
          super
          @oid_convertor_map = {}
          @type_convertor_map[:RubyPGArray] = TypeConvertor::INSTANCE.method(:RubyPGArray)
          @type_convertor_map[:RubyPGHstore] = TypeConvertor::INSTANCE.method(:RubyPGHstore)
        end
      end
      
      # Dataset subclass used for datasets that connect to PostgreSQL via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::Postgres::DatasetMethods
        APOS = Dataset::APOS
        
        private
        
        # Literalize strings similar to the native postgres adapter
        def literal_string_append(sql, v)
          sql << APOS << db.synchronize(@opts[:server]){|c| c.escape_string(v)} << APOS
        end

        STRING_TYPE = Java::JavaSQL::Types::VARCHAR
        ARRAY_TYPE = Java::JavaSQL::Types::ARRAY
        PG_SPECIFIC_TYPES = [ARRAY_TYPE, Java::JavaSQL::Types::OTHER, Java::JavaSQL::Types::STRUCT]
        HSTORE_TYPE = 'hstore'.freeze

        def type_convertor(map, meta, type, i)
          case type
          when *PG_SPECIFIC_TYPES
            oid = meta.getField(i).getOID
            if pr = db.oid_convertor_proc(oid)
              pr
            elsif type == ARRAY_TYPE
              map[:RubyPGArray]
            elsif oid == 2950 # UUID
              map[STRING_TYPE]
            elsif meta.getPGType(i) == HSTORE_TYPE
              map[:RubyPGHstore]
            else
              super
            end
          else
            super
          end
        end
      end
    end
  end
end
