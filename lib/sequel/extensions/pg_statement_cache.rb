# This extension adds a statement cache to Sequel's postgres adapter,
# with the ability to automatically prepare statements that are
# executed repeatedly.  When combined with the pg_auto_parameterize
# extension, it can take Sequel code such as:
#
#   DB.extension :pg_auto_parameterize, :pg_statement_cache
#   DB[:table].filter(:a=>1)
#   DB[:table].filter(:a=>2)
#   DB[:table].filter(:a=>3)
#
# And use the same prepared statement to execute the queries.
#
# The backbone of this extension is a modified LRU cache.  It considers
# both the last executed time and the number of executions when
# determining which queries to keep in the cache.  It only cleans the
# cache when a high water mark has been passed, and removes queries
# until it reaches the low water mark, in order to avoid thrashing when
# you are using more than the maximum number of queries.  To avoid
# preparing queries when it isn't necessary, it does not prepare them
# on the server side unless they are being executed more than once.
# The cache is very tunable, allowing you to set the high and low
# water marks, the number of executions before preparing the query,
# and even use a custom callback for determine which queries to keep
# in the cache.
#
# Note that automatically preparing statements does have some issues.
# Most notably, if you change the result type that the query returns,
# PostgreSQL will raise an error.  This can happen if you have
# prepared a statement that selects all columns from a table, and then
# you add or remove a column from that table.  This extension does
# attempt to check that case and clear the statement caches if you use
# alter_table from within Sequel, but it cannot fix the case when such
# a change is made externally.
#
# This extension only works when the pg driver is used as the backend
# for the postgres adapter.

module Sequel
  module Postgres
    module StatementCache
      # A simple structure used for the values in the StatementCache's hash.
      # It does not hold the related SQL, since that is used as the key for
      # the StatementCache's hash.
      class Statement
        # The last time this statement was seen by the cache, persumably the
        # last time it was executed.
        attr_accessor :last_seen

        # The total number of executions since the statement entered the cache.
        attr_accessor :num_executes

        # The id related to the statement, used as part of the prepared statement
        # name.
        attr_reader :cache_id

        # Used when adding entries to the cache, just sets their id.  Uses
        # 0 for num_executes since that is incremented elsewhere. Does not
        # set last_seen since that is set elsewhere to reduce branching.
        def initialize(cache_id)
          @num_executes = 0
          @cache_id = cache_id
        end

        # The name to use for the server side prepared statement.  Note that this
        # statement might not actually be prepared.
        def name
          "sequel_pgap_#{cache_id}"
        end
      end

      # The backbone of the block, a modified LRU (least recently used) cache
      # mapping SQL query strings to Statement objects.
      class StatementCache
        include Enumerable

        # Set the options for the statement cache.  These are generally set at
        # the database level using the :statement_cache_opts Database option.
        #
        # :max_size :: The maximum size (high water mark) for the cache.  If
        #              an entry is added when the current size of the cache is
        #              equal to the maximum size, the cache is cleaned up to
        #              reduce the number of entries to the :min_size.  Defaults
        #              to 1000.
        # :min_size :: The minimum size (low water mark) for the cache.  On
        #              cleanup, the size of the cache is reduced to this
        #              number.  Note that there could be fewer than this
        #              number of entries in the cache.  Defaults to :max_size/2. 
        # :prepare_after :: The number of executions to wait for before preparing
        #              the query server-side.  If set to 1, prepares all executed
        #              queries server-side.  If set to 5, does not attempt to
        #              prepare the query until the 5th execution.  Defaults to 2.
        # :sorter :: A callable object that takes two arguments, the current time
        #            and the related Statement instance, and should return some
        #            Comparable (usually a numeric) such that the lowest values
        #            returned are the first to be removed when it comes time to
        #            clean the pool.  The default is basically:
        #             
        #              lambda{|t, stmt| (stmt.last_seen - t)/stmt.num_executes}
        #
        #            so that it doesn't remove statements that have been executed
        #            many times just because many less-frequently executed statements 
        #            have been executed recently.
        #
        # The block passed is called with the Statement object's name, only for
        # statements that have been prepared, and should be used to deallocate the
        # statements.
        def initialize(opts={}, &block)
          @cleanup_proc = block
          @prepare_after = opts.fetch(:prepare_after, 2)
          @max_size = opts.fetch(:max_size, 1000)
          @min_size = opts.fetch(:min_size, @max_size/2)
          @sorter = opts.fetch(:sorter){method(:default_sorter)}
          @ids = (1..@max_size).to_a.reverse
          @hash = {}
          #
          # We add one so that when we clean the cache, the entry
          # about to be added brings us to the min_size.
          @size_diff = @max_size - @min_size + 1
        end
        
        # Completely clear the statement cache, deallocating on
        # the server side all statements that have been prepared.
        def clear
          @hash.keys.each{|k| remove(k)}
        end

        # Yield each SQL string and Statement instance in the cache
        # to the block.
        def each(&block)
          @hash.each(&block)
        end

        # Get the related statement name from the cache.  If the
        # entry is already in the cache, just bump it's last seen
        # time and the number of executions.  Otherwise, add it
        # to the cache.  If the cache is already full, clean it up
        # before adding it.
        #
        # If the num of executions has passed the threshhold, yield
        # the statement name to the block, which should be used to
        # prepare the statement on the server side.
        #
        # This method should return the prepared statment name if
        # the statement has been prepared, and nil if the query
        # has not been prepared and the statement should be executed
        # normally.
        def fetch(sql)
          unless stmt = @hash[sql]
            # Get the next id from the id pool.
            unless id = @ids.pop
              # No id left, cache must be full, so cleanup and then
              # get the next id from the id pool.
              cleanup
              id = @ids.pop
            end
            @hash[sql] = stmt = Statement.new(id)
          end

          stmt.last_seen = Time.now
          stmt.num_executes += 1

          if stmt.num_executes >= @prepare_after
            if stmt.num_executes == @prepare_after
              begin
                yield(stmt.name)
              rescue PGError
                # An error occurred while preparing the statement,
                # execute it normally (which will probably raise
                # the error again elsewhere), but decrement the
                # number of executions so we don't think we've
                # prepared the statement when we haven't.
                stmt.num_executes -= 1
                return nil
              end
            end
            stmt.name
          end
        end

        # The current size of the statement cache.
        def size
          @hash.length
        end

        private

        # Sort by time since last execution and number of executions.
        # We don't want to throw stuff out of the
        # cache if it has been executed a lot,
        # but a bunch of queries that were
        # executed only once came in more recently.
        def default_sorter(t, stmt)
          (stmt.last_seen - t)/stmt.num_executes
        end

        # After sorting the cache appropriately (so that the least important
        # items are first), reduce the number of entries in the cache to
        # the low water mark by removing the related query.  Should only be
        # called when the cache is full.
        def cleanup
          t = Time.now
          @hash.sort_by{|k,v| @sorter.call(t, v)}.first(@size_diff).each{|sql, stmt| remove(sql)}
        end

        # Remove the query from the cache.  If it has been prepared,
        # call the cleanup_proc to deallocate the statement.
        def remove(sql)
          stmt = @hash.delete(sql)
          if stmt.num_executes >= @prepare_after
            @cleanup_proc.call(stmt.name)
          end

          # Return id to the pool of ids
          @ids.push(stmt.cache_id)
        end
      end

      module AdapterMethods
        # A regular expression for the types of queries to cache.  Any queries not
        # matching this regular expression are not cached.
        DML_RE = /\A(WITH|SELECT|INSERT|UPDATE|DELETE) /

        # The StatementCache instance for this connection.  Note that
        # each connection has a separate StatementCache, because prepared
        # statements are connection-specific.
        attr_reader :statement_cache

        # Set the statement_cache for the connection, using the database's
        # :statement_cache_opts option.
        def self.extended(c)
          c.instance_variable_set(:@statement_cache, StatementCache.new(c.sequel_db.opts[:statement_cache_opts] || {}){|name| c.deallocate(name)})
        end

        # pg seems to already use the db method (but not the @db instance variable),
        # so use the sequel_db method to access the related Sequel::Database object.
        def sequel_db
          @db
        end

        # Deallocate on the server the prepared statement with the given name.
        def deallocate(name)
          begin
            execute("DEALLOCATE #{name}")
          rescue PGError
            # table probably got removed, just ignore it
          end
        end

        private

        # If the sql query string is one we should cache, cache it.  If the query already
        # has a related prepared statement with it, execute the prepared statement instead
        # of executing the query normally.
        def execute_query(sql, args=nil)
          if sql =~ DML_RE
            if name = statement_cache.fetch(sql){|stmt_name| sequel_db.log_yield("PREPARE #{stmt_name} AS #{sql}"){prepare(stmt_name, sql)}}
              if args
                sequel_db.log_yield("EXECUTE #{name} (#{sql})", args){exec_prepared(name, args)}
              else
                sequel_db.log_yield("EXECUTE #{name} (#{sql})"){exec_prepared(name)}
              end
            else
              super
            end
          else
            super
          end
        end
      end
      
      module DatabaseMethods
        # Setup the after_connect proc for the connection pool to make
        # sure the connection object is extended with the appropriate
        # module.  This disconnects any existing connections to ensure
        # that all connections in the pool have been extended appropriately.
        def self.extended(db)
          # Respect existing after_connect proc if one is present
          pr = db.opts[:after_connect]

          # Set the after_connect proc to extend the adapter with
          # the statement cache support.
          db.pool.after_connect = db.opts[:after_connect] = proc do |c|
            pr.call(c) if pr
            c.extend(AdapterMethods)
          end

          # Disconnect to make sure all connections get set up with
          # statement cache.
          db.disconnect
        end

        # Clear statement caches for all connections when altering tables.
        def alter_table(*)
          clear_statement_caches
          super
        end

        # Clear statement caches for all connections when dropping tables.
        def drop_table(*)
          clear_statement_caches
          super
        end

        private

        # Clear the statement cache for all connections.  Note that for
        # the threaded pools, this will not affect connections currently
        # allocated to other threads.
        def clear_statement_caches
          pool.all_connections{|c| c.statement_cache.clear}
        end
      end
    end
  end

  Database.register_extension(:pg_statement_cache, Postgres::StatementCache::DatabaseMethods)
end
Sequel::Deprecation.deprecate('The pg_statement_cache extension', 'Please stop loading it') unless defined?(SEQUEL_EXTENSIONS_NO_DEPRECATION_WARNING)
