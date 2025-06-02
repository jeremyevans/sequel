# frozen-string-literal: true
#
# The async_thread_pool extension adds support for running database
# queries in a separate threads using a thread pool. With the following
# code
#
#   DB.extension :async_thread_pool
#   foos = DB[:foos].async.where(name: 'A'..'M').all
#   bar_names = DB[:bar].async.select_order_map(:name)
#   baz_1 = DB[:bazes].async.first(id: 1)
#
# All 3 queries will be run in separate threads.  +foos+, +bar_names+
# and +baz_1+ will be proxy objects.  Calling a method on the proxy
# object will wait for the query to be run, and will return the result
# of calling that method on the result of the query method. For example,
# if you run:
#
#   foos = DB[:foos].async.where(name: 'A'..'M').all
#   bar_names = DB[:bars].async.select_order_map(:name)
#   baz_1 = DB[:bazes].async.first(id: 1)
#   sleep(1)
#   foos.size
#   bar_names.first
#   baz_1.name
#
# These three queries will generally be run concurrently in separate
# threads.  If you instead run:
#   
#   DB[:foos].async.where(name: 'A'..'M').all.size
#   DB[:bars].async.select_order_map(:name).first
#   DB[:bazes].async.first(id: 1).name
#
# Then will run each query sequentially, since you need the result of
# one query before running the next query.  The queries will still be
# run in separate threads (by default).
#
# What is run in the separate thread is the entire method call that
# returns results.  So with the original example:
#
#   foos = DB[:foos].async.where(name: 'A'..'M').all
#   bar_names = DB[:bars].async.select_order_map(:name)
#   baz_1 = DB[:bazes].async.first(id: 1)
#
# The +all+, <tt>select_order_map(:name)</tt>, and <tt>first(id: 1)</tt>
# calls are run in separate threads.  If a block is passed to a method
# such as +all+ or +each+, the block is also run in that thread.  If you
# have code such as:
#
#   h = {}
#   DB[:foos].async.each{|row| h[row[:id]] = row}
#   bar_names = DB[:bars].async.select_order_map(:name)
#   p h
#
# You may end up with it printing an empty hash or partial hash, because the
# async +each+ call will not have run or finished running.  Since the
# <tt>p h</tt> code relies on a side-effect of the +each+ block and not the
# return value of the +each+ call, it will not wait for the loading.
#
# You should avoid using +async+ for any queries where you are ignoring the
# return value, as otherwise you have no way to wait for the query to be run.
#
# Datasets that use async will use async threads to load data for the majority
# of methods that can return data.  However, dataset methods that return
# enumerators will not use an async thread (e.g. calling # Dataset#map
# without a block or arguments does not use an async thread or return a
# proxy object).
#
# Because async methods (including their blocks) run in a separate thread, you
# should not use control flow modifiers such as +return+ or +break+ in async
# queries.  Doing so will result in a error.
#
# Because async results are returned as proxy objects, it's a bad idea
# to use them in a boolean setting:
#
#   result = DB[:foo].async.get(:boolean_column)
#   # or:
#   result = DB[:foo].async.first
#
#   # ...
#   if result 
#     # will always execute this banch, since result is a proxy object
#   end
#
# In this case, you can call the +__value+ method to return the actual
# result:
#
#   if result.__value
#     # will not execute this branch if the dataset method returned nil or false
#   end
#
# Similarly, because a proxy object is used, you should be careful using the
# result in a case statement or an argument to <tt>Class#===</tt>:
#
#   # ...
#   case result
#   when Hash, true, false
#     # will never take this branch, since result is a proxy object
#   end
#
# Similar to usage in an +if+ statement, you should use +__value+:
#
#   case result.__value
#   when Hash, true, false
#     # will never take this branch, since result is a proxy object
#   end
#
# On Ruby 2.2+, you can use +itself+ instead of +__value+.  It's preferable to
# use +itself+ if you can, as that will allow code to work with both proxy
# objects and regular objects.
#
# Because separate threads and connections are used for async queries,
# they do not use any state on the current connection/thread. So if
# you do:
#
#   DB.transaction{DB[:table].async.all}
#
# Be aware that the transaction runs on one connection, and the SELECT
# query on a different connection.  If you use currently using
# transactional testing (running each test inside a transaction/savepoint),
# and want to start using this extension, you should first switch to
# non-transactional testing of the code that will use the async thread
# pool before using this extension, as otherwise the use of
# <tt>Dataset#async</tt> will likely break your tests.
#
# If you are using Database#synchronize to checkout a connection, the
# same issue applies, where the async query runs on a different
# connection:
#
#   DB.synchronize{DB[:table].async.all}
# 
# Similarly, if you are using the server_block extension, any async
# queries inside with_server blocks will not use the server specified:
#
#   DB.with_server(:shard1) do
#     DB[:a].all # Uses shard1
#     DB[:a].async.all # Uses default shard
#   end
#
# You need to manually specify the shard for any dataset using an async
# query:
# 
#   DB.with_server(:shard1) do
#     DB[:a].all # Uses shard1
#     DB[:a].async.server(:shard1).all # Uses shard1
#   end
#
# When the async_thread_pool extension, the size of the async thread pool
# can be set by using the +:num_async_threads+ Database option, which must
# be set before loading the async_thread_pool extension.  This defaults
# to the size of the Database object's connection pool.
#
# By default, for consistent behavior, the async_thread_pool extension
# will always run the query in a separate thread. However, in some cases,
# such as when the async thread pool is busy and the results of a query
# are needed right away, it can improve performance to allow preemption,
# so that the query will run in the current thread instead of waiting
# for an async thread to become available.  With the following code:
#
#   foos = DB[:foos].async.where(name: 'A'..'M').all
#   bar_names = DB[:bar].async.select_order_map(:name)
#   if foos.length > 4
#     baz_1 = DB[:bazes].async.first(id: 1)
#   end
# 
# Whether you need the +baz_1+ variable depends on the value of foos.
# If the async thread pool is busy, and by the time the +foos.length+
# call is made, the async thread pool has not started the processing
# to get the +foos+ value, it can improve performance to start that
# processing in the current thread, since it is needed immediately to
# determine whether to schedule query to get the +baz_1+ variable.
# The default is to not allow preemption, because if the current
# thread is used, it may have already checked out a connection that
# could be used, and that connection could be inside a transaction or
# have some other manner of connection-specific state applied to it.
# If you want to allow preemption, you can set the
# +:preempt_async_thread+ Database option before loading the
# async_thread_pool extension.
#
# Note that the async_thread_pool extension creates the thread pool
# when it is loaded into the Database.  If you fork after loading
# the extension, the extension will not work, as fork does not
# copy the thread pools.  If you are using a forking webserver
# (or any other system that forks worker processes), load this
# extension in each child process, do not load it before forking.
#
# Related module: Sequel::Database::AsyncThreadPool::DatasetMethods


# 
module Sequel
  module Database::AsyncThreadPool
    # JobProcessor is a wrapper around a single thread, that will
    # process a queue of jobs until it is shut down.
    class JobProcessor # :nodoc:
      def self.create_finalizer(queue, pool)
        proc{run_finalizer(queue, pool)}
      end

      def self.run_finalizer(queue, pool)
        # Push a nil for each thread using the queue, signalling
        # that thread to close.
        pool.each{queue.push(nil)}

        # Join each of the closed threads.
        pool.each(&:join)

        # Clear the thread pool.  Probably not necessary, but this allows
        # for a simple way to check whether this finalizer has been run.
        pool.clear

        nil
      end
      private_class_method :run_finalizer

      def initialize(queue)
        @thread = ::Thread.new do
          while proxy = queue.pop
            proxy.__send__(:__run)
          end
        end
      end

      # Join the thread, should only be called by the related finalizer.
      def join
        @thread.join
      end
    end

    # Wrapper for exception instances raised by async jobs.  The
    # wrapped exception will be raised by the code getting the value
    # of the job.
    WrappedException = Struct.new(:exception)

    # Base proxy object class for jobs processed by async threads and
    # the returned result.
    class BaseProxy < BasicObject
      # Store a block that returns the result when called.
      def initialize(&block)
        ::Kernel.raise Error, "must provide block for an async job" unless block
        @block = block
      end

      # Pass all method calls to the returned result.
      def method_missing(*args, &block)
        __value.public_send(*args, &block)
      end
      # :nocov:
      ruby2_keywords(:method_missing) if respond_to?(:ruby2_keywords, true)
      # :nocov:

      # Delegate respond_to? calls to the returned result.
      def respond_to_missing?(*args)
        __value.respond_to?(*args)
      end

      # Override some methods defined by default so they apply to the
      # returned result and not the current object.
      [:!, :==, :!=, :instance_eval, :instance_exec].each do |method|
        define_method(method) do |*args, &block|
          __value.public_send(method, *args, &block)
        end
      end

      # Wait for the value to be loaded if it hasn't already been loaded.
      # If the code to load the return value raised an exception that was
      # wrapped, reraise the exception.
      def __value
        unless defined?(@value)
          __get_value
        end

        if @value.is_a?(WrappedException)
          ::Kernel.raise @value
        end

        @value
      end

      private

      # Run the block and return the block value.  If the block call raises
      # an exception, wrap the exception.
      def __run_block
        # This may not catch concurrent calls (unless surrounded by a mutex), but
        # it's not worth trying to protect against that.  It's enough to just check for
        # multiple non-concurrent calls.
        ::Kernel.raise Error, "Cannot run async block multiple times" unless block = @block

        @block = nil

        begin
          block.call
        rescue ::Exception => e
          WrappedException.new(e)
        end
      end
    end

    # Default object class for async job/proxy result.  This uses a queue for
    # synchronization.  The JobProcessor will push a result until the queue,
    # and the code to get the value will pop the result from that queue (and
    # repush the result to handle thread safety).
    class Proxy < BaseProxy
      def initialize
        super
        @queue = ::Queue.new
      end

      private

      def __run
        @queue.push(__run_block)
      end

      def __get_value
        @value = @queue.pop

        # Handle thread-safety by repushing the popped value, so that
        # concurrent calls will receive the same value
        @queue.push(@value)
      end
    end

    # Object class for async job/proxy result when the :preempt_async_thread
    # Database option is used.  Uses a mutex for synchronization, and either
    # the JobProcessor or the calling thread can run code to get the value.
    class PreemptableProxy < BaseProxy
      def initialize
        super
        @mutex = ::Mutex.new
      end

      private

      def __get_value
        @mutex.synchronize do
          unless defined?(@value)
            @value = __run_block
          end
        end
      end
      alias __run __get_value
    end

    module DatabaseMethods
      def self.extended(db)
        db.instance_exec do
          case pool.pool_type
          when :single, :sharded_single
            raise Error, "cannot load async_thread_pool extension if using single or sharded_single connection pool"
          end

          num_async_threads = opts[:num_async_threads] ? typecast_value_integer(opts[:num_async_threads]) : (Integer(opts[:max_connections] || 4))
          raise Error, "must have positive number for num_async_threads" if num_async_threads <= 0

          proxy_klass = typecast_value_boolean(opts[:preempt_async_thread]) ? PreemptableProxy : Proxy
          define_singleton_method(:async_job_class){proxy_klass}

          queue = @async_thread_queue = Queue.new
          pool = @async_thread_pool = Array.new(num_async_threads){JobProcessor.new(queue)}
          ObjectSpace.define_finalizer(db, JobProcessor.create_finalizer(queue, pool))

          extend_datasets(DatasetMethods)
        end
      end

      private

      # Wrap the block in a job/proxy object and schedule it to run using the async thread pool.
      def async_run(&block)
        proxy = async_job_class.new(&block)
        @async_thread_queue.push(proxy)
        proxy
      end
    end

    ASYNC_METHODS = ([:all?, :any?, :drop, :entries, :grep_v, :include?, :inject, :member?, :minmax, :none?, :one?, :reduce, :sort, :take, :tally, :to_a, :to_h, :uniq, :zip] & Enumerable.instance_methods) + (Dataset::ACTION_METHODS - [:map, :paged_each])
    ASYNC_BLOCK_METHODS = ([:collect, :collect_concat, :detect,  :drop_while, :each_cons, :each_entry, :each_slice, :each_with_index, :each_with_object, :filter_map, :find, :find_all, :find_index, :flat_map, :max_by, :min_by, :minmax_by, :partition, :reject, :reverse_each, :sort_by, :take_while] & Enumerable.instance_methods) + [:paged_each]
    ASYNC_ARGS_OR_BLOCK_METHODS = [:map]

    module DatasetMethods
      # Define an method in the given module that will run the given method using an async thread
      # if the current dataset is async.
      def self.define_async_method(mod, method)
        mod.send(:define_method, method) do |*args, &block|
          if @opts[:async]
            ds = sync
            db.send(:async_run){ds.send(method, *args, &block)}
          else
            super(*args, &block)
          end
        end
      end

      # Define an method in the given module that will run the given method using an async thread
      # if the current dataset is async and a block is provided.
      def self.define_async_block_method(mod, method)
        mod.send(:define_method, method) do |*args, &block|
          if block && @opts[:async]
            ds = sync
            db.send(:async_run){ds.send(method, *args, &block)}
          else
            super(*args, &block)
          end
        end
      end

      # Define an method in the given module that will run the given method using an async thread
      # if the current dataset is async and arguments or a block is provided.
      def self.define_async_args_or_block_method(mod, method)
        mod.send(:define_method, method) do |*args, &block|
          if (block || !args.empty?) && @opts[:async]
            ds = sync
            db.send(:async_run){ds.send(method, *args, &block)}
          else
            super(*args, &block)
          end
        end
      end

      # Override all of the methods that return results to do the processing in an async thread
      # if they have been marked to run async and should run async (i.e. they don't return an
      # Enumerator).
      ASYNC_METHODS.each{|m| define_async_method(self, m)}
      ASYNC_BLOCK_METHODS.each{|m| define_async_block_method(self, m)}
      ASYNC_ARGS_OR_BLOCK_METHODS.each{|m| define_async_args_or_block_method(self, m)}

      # Return a cloned dataset that will load results using the async thread pool.
      def async
        cached_dataset(:_async) do
          clone(:async=>true)
        end
      end

      # Return a cloned dataset that will not load results using the async thread pool.
      # Only used if the current dataset has been marked as using the async thread pool.
      def sync
        cached_dataset(:_sync) do
          clone(:async=>false)
        end
      end
    end
  end

  Database.register_extension(:async_thread_pool, Database::AsyncThreadPool::DatabaseMethods)
end
