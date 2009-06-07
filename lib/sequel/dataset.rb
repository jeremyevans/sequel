module Sequel
  # A dataset represents an SQL query, or more generally, an abstract
  # set of rows in the database.  Datasets
  # can be used to create, retrieve, update and delete records.
  # 
  # Query results are always retrieved on demand, so a dataset can be kept
  # around and reused indefinitely (datasets never cache results):
  #
  #   my_posts = DB[:posts].filter(:author => 'david') # no records are retrieved
  #   my_posts.all # records are retrieved
  #   my_posts.all # records are retrieved again
  #
  # Most dataset methods return modified copies of the dataset (functional style), so you can
  # reuse different datasets to access data:
  #
  #   posts = DB[:posts]
  #   davids_posts = posts.filter(:author => 'david')
  #   old_posts = posts.filter('stamp < ?', Date.today - 7)
  #   davids_old_posts = davids_posts.filter('stamp < ?', Date.today - 7)
  #
  # Datasets are Enumerable objects, so they can be manipulated using any
  # of the Enumerable methods, such as map, inject, etc.
  #
  # === Methods added via metaprogramming
  #
  # Some methods are added via metaprogramming:
  #
  # * ! methods - These methods are the same as their non-! counterparts,
  #   but they modify the receiver instead of returning a modified copy
  #   of the dataset.
  # * inner_join, full_outer_join, right_outer_join, left_outer_join - 
  #   This methods are shortcuts to join_table with the join type
  #   already specified.
  class Dataset
    extend Metaprogramming
    include Metaprogramming
    include Enumerable
    
    # The dataset options that require the removal of cached columns
    # if changed.
    COLUMN_CHANGE_OPTS = [:select, :sql, :from, :join].freeze

    # All methods that should have a ! method added that modifies
    # the receiver.
    MUTATION_METHODS = %w'add_graph_aliases and distinct exclude exists
    filter from from_self full_outer_join graph
    group group_and_count group_by having inner_join intersect invert join
    left_outer_join limit naked or order order_by order_more paginate query reject
    reverse reverse_order right_outer_join select select_all select_more
    set_defaults set_graph_aliases set_overrides sort sort_by
    unfiltered union unordered where with_sql'.collect{|x| x.to_sym}

    NOTIMPL_MSG = "This method must be overridden in Sequel adapters".freeze

    # The database that corresponds to this dataset
    attr_accessor :db
    
    # Set the method to call on identifiers going into the database for this dataset
    attr_accessor :identifier_input_method
    
    # Set the method to call on identifiers coming the database for this dataset
    attr_accessor :identifier_output_method

    # The hash of options for this dataset, keys are symbols.
    attr_accessor :opts

    # Whether to quote identifiers for this dataset
    attr_writer :quote_identifiers
    
    # The row_proc for this database, should be a Proc that takes
    # a single hash argument and returns the object you want
    # each to return.
    attr_accessor :row_proc

    # Constructs a new Dataset instance with an associated database and 
    # options. Datasets are usually constructed by invoking the Database#[] method:
    #
    #   DB[:posts]
    #
    # Sequel::Dataset is an abstract class that is not useful by itself. Each
    # database adaptor should provide a subclass of Sequel::Dataset, and have
    # the Database#dataset method return an instance of that class.
    def initialize(db, opts = nil)
      @db = db
      @quote_identifiers = db.quote_identifiers? if db.respond_to?(:quote_identifiers?)
      @identifier_input_method = db.identifier_input_method if db.respond_to?(:identifier_input_method)
      @identifier_output_method = db.identifier_output_method if db.respond_to?(:identifier_output_method)
      @opts = opts || {}
      @row_proc = nil
    end
    
    ### Class Methods ###

    # Setup mutation (e.g. filter!) methods.  These operate the same as the
    # non-! methods, but replace the options of the current dataset with the
    # options of the resulting dataset.
    def self.def_mutation_method(*meths)
      meths.each do |meth|
        class_eval("def #{meth}!(*args, &block); mutation_method(:#{meth}, *args, &block) end")
      end
    end

    ### Instance Methods ###

    # Alias for insert, but not aliased directly so subclasses
    # don't have to override both methods.
    def <<(*args)
      insert(*args)
    end

    # Return the dataset as an aliased expression with the given alias. You can
    # use this as a FROM or JOIN dataset, or as a column if this dataset
    # returns a single row and column.
    def as(aliaz)
      ::Sequel::SQL::AliasedExpression.new(self, aliaz)
    end

    # Returns an array with all records in the dataset. If a block is given,
    # the array is iterated over after all items have been loaded.
    def all(&block)
      a = []
      each{|r| a << r}
      post_load(a)
      a.each(&block) if block
      a
    end
  
    # Returns a new clone of the dataset with with the given options merged.
    # If the options changed include options in COLUMN_CHANGE_OPTS, the cached
    # columns are deleted.
    def clone(opts = {})
      c = super()
      c.opts = @opts.merge(opts)
      c.instance_variable_set(:@columns, nil) if opts.keys.any?{|o| COLUMN_CHANGE_OPTS.include?(o)}
      c
    end
    
    # Returns the columns in the result set in order.
    # If the columns are currently cached, returns the cached value. Otherwise,
    # a SELECT query is performed to get a single row. Adapters are expected
    # to fill the columns cache with the column information when a query is performed.
    # If the dataset does not have any rows, this may be an empty array depending on how
    # the adapter is programmed.
    #
    # If you are looking for all columns for a single table and maybe some information about
    # each column (e.g. type), see Database#schema.
    def columns
      return @columns if @columns
      ds = unfiltered.unordered.clone(:distinct => nil, :limit => 1)
      ds.each{break}
      @columns = ds.instance_variable_get(:@columns)
      @columns || []
    end
    
    # Remove the cached list of columns and do a SELECT query to find
    # the columns.
    def columns!
      @columns = nil
      columns
    end
    
    # Add a mutation method to this dataset instance.
    def def_mutation_method(*meths)
      meths.each do |meth|
        instance_eval("def #{meth}!(*args, &block); mutation_method(:#{meth}, *args, &block) end")
      end
    end

    # Deletes the records in the dataset.  The returned value is generally the
    # number of records deleted, but that is adapter dependent.
    def delete
      execute_dui(delete_sql)
    end
    
    # Iterates over the records in the dataset as they are yielded from the
    # database adapter, and returns self.
    def each(&block)
      if @opts[:graph]
        graph_each(&block)
      else
        if row_proc = @row_proc
          fetch_rows(select_sql){|r| yield row_proc.call(r)}
        else
          fetch_rows(select_sql, &block)
        end
      end
      self
    end

    # Executes a select query and fetches records, passing each record to the
    # supplied block.  The yielded records should be hashes with symbol keys.
    def fetch_rows(sql, &block)
      raise NotImplementedError, NOTIMPL_MSG
    end
  
    # Inserts values into the associated table.  The returned value is generally
    # the value of the primary key for the inserted row, but that is adapter dependent.
    def insert(*values)
      execute_insert(insert_sql(*values))
    end
  
    # Returns a string representation of the dataset including the class name 
    # and the corresponding SQL select statement.
    def inspect
      "#<#{self.class}: #{sql.inspect}>"
    end

    # Returns a naked dataset clone - i.e. a dataset that returns records as
    # hashes instead of calling the row proc.
    def naked
      ds = clone
      ds.row_proc = nil
      ds
    end
    
    # Whether this dataset quotes identifiers.
    def quote_identifiers?
      @quote_identifiers
    end
    
    # Set the server for this dataset to use.  Used to pick a specific database
    # shard to run a query against, or to override the default (which is SELECT uses
    # :read_only database and all other queries use the :default database).
    def server(servr)
      clone(:server=>servr)
    end

    # Alias for set, but not aliased directly so subclasses
    # don't have to override both methods.
    def set(*args)
      update(*args)
    end

    # Set the default values for insert and update statements.  The values passed
    # to insert or update are merged into this hash.
    def set_defaults(hash)
      clone(:defaults=>(@opts[:defaults]||{}).merge(hash))
    end

    # Set values that override hash arguments given to insert and update statements.
    # This hash is merged into the hash provided to insert or update.
    def set_overrides(hash)
      clone(:overrides=>hash.merge(@opts[:overrides]||{}))
    end

    # Updates values for the dataset.  The returned value is generally the
    # number of rows updated, but that is adapter dependent.
    def update(values={})
      execute_dui(update_sql(values))
    end
  
    # Add the mutation methods via metaprogramming
    def_mutation_method(*MUTATION_METHODS)

    protected

    # Return true if the dataset has a non-nil value for any key in opts.
    def options_overlap(opts)
      !(@opts.collect{|k,v| k unless v.nil?}.compact & opts).empty?
    end

    # Whether this dataset is a simple SELECT * FROM table.
    def simple_select_all?
      o = @opts.reject{|k,v| v.nil?}
      o.length == 1 && (f = o[:from]) && f.length == 1 && f.first.is_a?(Symbol)
    end

    private
    
    # Set the server to use to :default unless it is already set in the passed opts
    def default_server_opts(opts)
      {:server=>@opts[:server] || :default}.merge(opts)
    end

    # Execute the given SQL on the database using execute.
    def execute(sql, opts={}, &block)
      @db.execute(sql, {:server=>@opts[:server] || :read_only}.merge(opts), &block)
    end
    
    # Execute the given SQL on the database using execute_dui.
    def execute_dui(sql, opts={}, &block)
      @db.execute_dui(sql, default_server_opts(opts), &block)
    end
    
    # Execute the given SQL on the database using execute_insert.
    def execute_insert(sql, opts={}, &block)
      @db.execute_insert(sql, default_server_opts(opts), &block)
    end
    
    # Modify the identifier returned from the database based on the
    # identifier_output_method.
    def input_identifier(v)
      (i = identifier_input_method) ? v.to_s.send(i) : v.to_s
    end

    # Modify the receiver with the results of sending the meth, args, and block
    # to the receiver and merging the options of the resulting dataset into
    # the receiver's options.
    def mutation_method(meth, *args, &block)
      copy = send(meth, *args, &block)
      @opts.merge!(copy.opts)
      self
    end
    
    # Modify the identifier returned from the database based on the
    # identifier_output_method.
    def output_identifier(v)
      (i = identifier_output_method) ? v.to_s.send(i).to_sym : v.to_sym
    end

    # This is run inside .all, after all of the records have been loaded
    # via .each, but before any block passed to all is called.  It is called with
    # a single argument, an array of all returned records.  Does nothing by
    # default, added to make the model eager loading code simpler.
    def post_load(all_records)
    end

    # If a block argument is passed to a method that uses a VirtualRow,
    # yield a new VirtualRow instance to the block if it accepts a single
    # argument.  Otherwise, evaluate the block in the context of a new
    # VirtualRow instance.
    def virtual_row_block_call(block)
      return unless block
      case block.arity
      when -1, 0
        SQL::VirtualRow.new.instance_eval(&block)
      else
        block.call(SQL::VirtualRow.new)
      end
    end
  end
end
