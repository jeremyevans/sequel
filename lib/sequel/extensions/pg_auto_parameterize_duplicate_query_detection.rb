# frozen-string-literal: true
#
# The pg_auto_parameterize_duplicate_query_detection extension builds on the
# pg_auto_parameterize extension, adding support for detecting duplicate
# queries inside a block that occur at the same location. This is designed
# mostly to catch duplicate query issues (e.g. N+1 queries) during testing.
#
# To detect duplicate queries inside a block of code, wrap the code with
# +detect_duplicate_queries+:
#
#   DB.detect_duplicate_queries{your_code}
#
# With this approach, if the test runs code where the same query is executed
# more than once with the same call stack, a
# Sequel::Postgres::AutoParameterizeDuplicateQueryDetection::DuplicateQueries
# exception will be raised.
#
# You can pass the +:warn+ option to +detect_duplicate_queries+ to warn
# instead of raising. Note that if the block passed to +detect_duplicate_queries+
# raises, this extension will warn, and raise the original exception.
#
# For more control, you can pass the +:handler+ option to
# +detect_duplicate_queries+. If the +:handler+ option is provided, this
# extension will call the +:handler+ option with the hash of duplicate
# query information, and will not raise or warn. This can be useful in
# production environments, to record duplicate queries for later analysis.
#
# For accuracy, the entire call stack is always used as part of the hash key
# to determine whether a query is duplicate. However, you can filter the
# displayed backtrace by using the +:backtrace_filter+ option.
#
# +detect_duplicate_queries+ is concurrency aware, it uses the same approach
# that Sequel's default connection pools use. So if you are running multiple
# threads, +detect_duplicate_queries+ will only report duplicate queries for
# the current thread (or fiber if the fiber_concurrency extension is used).
#
# When testing applications, it's probably best to use this to wrap the
# application being tested. For example, testing with rack-test, if your app
# is +App+, you would want to wrap it:
#
#   include Rack::Test::Methods
#
#   WrappedApp = lambda do |env|
#     DB.detect_duplicate_queries{App.call(env)}
#   end
#
#   def app
#     WrappedApp
#   end
#
# It is possible to use this to wrap each separate spec using an around hook,
# but that can result in false positives when using libraries that have
# implicit retrying (such as Capybara), as you can have the same call stack
# for multiple requests.
#
# Related module: Sequel::Postgres::AutoParameterizeDuplicateQueryDetection

module Sequel
  module Postgres
    # Enable detecting duplicate queries inside a block
    module AutoParameterizeDuplicateQueryDetection
      def self.extended(db)
        db.instance_exec do
          @duplicate_query_detection_contexts = {}
          @duplicate_query_detection_mutex = Mutex.new
        end
      end

      # Exception class raised when duplicate queries are detected.
      class DuplicateQueries < Sequel::Error
        # A hash of queries that were duplicate. Keys are arrays
        # with 2 entries, the first being the query SQL, and the
        # second being the related caller line.
        # The values are the number of query executions.
        attr_reader :queries

        def initialize(message, queries)
          @queries = queries
          super(message)
        end
      end

      # Record each query executed so duplicates can be detected,
      # if queries are being recorded.
      def execute(sql, opts=OPTS, &block)
        record, queries = duplicate_query_recorder_state

        if record
          queries[[sql.is_a?(Symbol) ? sql : sql.to_s, caller].freeze] += 1
        end

        super
      end

      # Ignore (do not record) queries inside given block. This can
      # be useful in situations where you want to run your entire test suite
      # with duplicate query detection, but you have duplicate queries in
      # some parts of your application where it is not trivial to use a
      # different approach. You can mark those specific sections with
      # +ignore_duplicate_queries+, and still get duplicate query detection
      # for the rest of the application.
      def ignore_duplicate_queries(&block)
        if state = duplicate_query_recorder_state
          change_duplicate_query_recorder_state(state, false, &block)
        else
          # If we are not inside a detect_duplicate_queries block, there is
          # no need to do anything, since we are not recording queries.
          yield
        end
      end

      # Run the duplicate query detector during the block.
      # Options:
      #
      # :backtrace_filter :: Regexp used to filter the displayed backtrace.
      # :handler :: If present, called with hash of duplicate query information,
      #             instead of raising or warning.
      # :warn :: Always warn instead of raising for duplicate queries.
      #
      # Note that if you nest calls to this method, only the top
      # level call will respect the passed options.
      def detect_duplicate_queries(opts=OPTS, &block)
        current = Sequel.current
        if state = duplicate_query_recorder_state(current)
          return change_duplicate_query_recorder_state(state, true, &block)
        end

        @duplicate_query_detection_mutex.synchronize do
          @duplicate_query_detection_contexts[current] = [true, Hash.new(0)]
        end

        begin
          yield
        rescue Exception => e
          raise
        ensure
          _, queries = @duplicate_query_detection_mutex.synchronize do
            @duplicate_query_detection_contexts.delete(current)
          end
          queries.delete_if{|_,v| v < 2}

          unless queries.empty?
            if handler = opts[:handler]
              handler.call(queries)
            else
              backtrace_filter = opts[:backtrace_filter]
              backtrace_filter_note = backtrace_filter ? " (filtered)" : ""
              query_info = queries.map do |k,v|
                backtrace = k[1]
                backtrace = backtrace.grep(backtrace_filter) if backtrace_filter
                "times:#{v}\nsql:#{k[0]}\nbacktrace#{backtrace_filter_note}:\n#{backtrace.join("\n")}\n"
              end
              message = "duplicate queries detected:\n\n#{query_info.join("\n")}"

              if e || opts[:warn]
                warn(message)
              else
                raise DuplicateQueries.new(message, queries)
              end
            end
          end
        end
      end

      private

      # Get the query record state for the given context.
      def duplicate_query_recorder_state(current=Sequel.current)
        @duplicate_query_detection_mutex.synchronize{@duplicate_query_detection_contexts[current]}
      end

      # Temporarily change whether to record queries for the block, resetting the
      # previous setting after the block exits.
      def change_duplicate_query_recorder_state(state, setting)
        prev = state[0]
        state[0] = setting
        
        begin
          yield
        ensure
          state[0] = prev
        end
      end
    end
  end

  Database.register_extension(:pg_auto_parameterize_duplicate_query_detection) do |db|
    db.extension(:pg_auto_parameterize)
    db.extend(Postgres::AutoParameterizeDuplicateQueryDetection)
  end
end
