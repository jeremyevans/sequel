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
  class Dataset
    extend Metaprogramming
    include Metaprogramming
    include Enumerable
    
    # The dataset options that require the removal of cached columns
    # if changed.
    COLUMN_CHANGE_OPTS = [:select, :sql, :from, :join].freeze

    # All methods that should have a ! method added that modifies
    # the receiver.
    MUTATION_METHODS = %w'add_graph_aliases and distinct except exclude
    filter from from_self full_outer_join graph
    group group_and_count group_by having inner_join intersect invert join join_table
    left_outer_join limit naked or order order_by order_more paginate qualify query
    reverse reverse_order right_outer_join select select_all select_more server
    set_defaults set_graph_aliases set_overrides unfiltered ungraphed ungrouped union
    unlimited unordered where with with_recursive with_sql'.collect{|x| x.to_sym}

    # Which options don't affect the SQL generation.  Used by simple_select_all?
    # to determine if this is a simple SELECT * FROM table.
    NON_SQL_OPTIONS = [:server, :defaults, :overrides, :graph, :eager_graph, :graph_aliases]

    NOTIMPL_MSG = "This method must be overridden in Sequel adapters".freeze
    WITH_SUPPORTED=:select_with_sql

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
        class_eval("def #{meth}!(*args, &block); mutation_method(:#{meth}, *args, &block) end", __FILE__, __LINE__)
      end
    end

    ### Instance Methods ###

    # Return the dataset as an aliased expression with the given alias. You can
    # use this as a FROM or JOIN dataset, or as a column if this dataset
    # returns a single row and column.
    def as(aliaz)
      ::Sequel::SQL::AliasedExpression.new(self, aliaz)
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
    
    # Add a mutation method to this dataset instance.
    def def_mutation_method(*meths)
      meths.each do |meth|
        instance_eval("def #{meth}!(*args, &block); mutation_method(:#{meth}, *args, &block) end", __FILE__, __LINE__)
      end
    end

    # Yield a dataset for each server in the connection pool that is tied to that server.
    # Intended for use in sharded environments where all servers need to be modified
    # with the same data:
    #
    #   DB[:configs].where(:key=>'setting').each_server{|ds| ds.update(:value=>'new_value')}
    def each_server
      db.servers.each{|s| yield server(s)}
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
    
    # Set the server for this dataset to use.  Used to pick a specific database
    # shard to run a query against, or to override the default (which is SELECT uses
    # :read_only database and all other queries use the :default database).
    def server(servr)
      clone(:server=>servr)
    end

    # Set the default values for insert and update statements.  The values hash passed
    # to insert or update are merged into this hash.
    def set_defaults(hash)
      clone(:defaults=>(@opts[:defaults]||{}).merge(hash))
    end

    # Set values that override hash arguments given to insert and update statements.
    # This hash is merged into the hash provided to insert or update.
    def set_overrides(hash)
      clone(:overrides=>hash.merge(@opts[:overrides]||{}))
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
      o = @opts.reject{|k,v| v.nil? || NON_SQL_OPTIONS.include?(k)}
      o.length == 1 && (f = o[:from]) && f.length == 1 && f.first.is_a?(Symbol)
    end

    private
    
    # Set the server to use to :default unless it is already set in the passed opts
    def default_server_opts(opts)
      {:server=>@opts[:server] || :default}.merge(opts)
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
      v = 'untitled' if v == ''
      (i = identifier_output_method) ? v.to_s.send(i).to_sym : v.to_sym
    end

    # This is run inside .all, after all of the records have been loaded
    # via .each, but before any block passed to all is called.  It is called with
    # a single argument, an array of all returned records.  Does nothing by
    # default, added to make the model eager loading code simpler.
    def post_load(all_records)
    end
  end
end
