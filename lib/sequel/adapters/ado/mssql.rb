Sequel.require 'adapters/shared/mssql'

module Sequel
  module ADO
    # Database and Dataset instance methods for MSSQL specific
    # support via ADO.
    module MSSQL
      module DatabaseMethods
        include Sequel::MSSQL::DatabaseMethods
        # Query to use to get the number of rows affected by an update or
        # delete query.
        ROWS_AFFECTED = "SELECT @@ROWCOUNT AS AffectedRows"

        # Just execute so it doesn't attempt to return the number of rows modified.
        def execute_ddl(sql, opts={})
          execute(sql, opts)
        end
        alias execute_insert execute_ddl

        # Issue a separate query to get the rows modified.  ADO appears to
        # use pass by reference with an integer variable, which is obviously
        # not supported directly in ruby, and I'm not aware of a workaround.
        def execute_dui(sql, opts={})
          return super unless @opts[:provider]
          synchronize(opts[:server]) do |conn|
            begin
              log_yield(sql){conn.Execute(sql)}
              res = log_yield(ROWS_AFFECTED){conn.Execute(ROWS_AFFECTED)}
              res.getRows.transpose.each{|r| return r.shift}
            rescue ::WIN32OLERuntimeError => e
              raise_error(e)
            end
          end
        end

        private

        # The ADO adapter's default provider doesn't support transactions, since it
        # creates a new native connection for each query.  So Sequel only attempts
        # to use transactions if an explicit :provider is given.
        def begin_transaction(conn, opts={})
          super if @opts[:provider]
        end

        def commit_transaction(conn, opts={})
          super if @opts[:provider]
        end

        def rollback_transaction(conn, opts={})
          super if @opts[:provider]
        end
      end

      class Dataset < ADO::Dataset
        include Sequel::MSSQL::DatasetMethods

        # Use a nasty hack of multiple SQL statements in the same call and
        # having the last one return the most recently inserted id.  This
        # is necessary as ADO's default :provider uses a separate native
        # connection for each query.
        def insert(*values)
          return super if @opts[:sql]
          with_sql("SET NOCOUNT ON; #{insert_sql(*values)}; SELECT CAST(SCOPE_IDENTITY() AS INTEGER)").single_value
        end

        # If you use a better :provider option for the database, you can get an
        # accurate number of rows matched.
        def provides_accurate_rows_matched?
          !!db.opts[:provider]
        end
      end
    end
  end
end
