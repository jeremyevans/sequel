module Sequel
  class Model
    # Add dataset methods via metaprogramming
    DATASET_METHODS = %w'all avg count delete distinct eager eager_graph each each_page 
       empty? except exclude filter first from_self full_outer_join graph 
       group group_and_count group_by having import inner_join insert 
       insert_multiple intersect interval invert_order join join_table last 
       left_outer_join limit multi_insert naked order order_by order_more 
       paginate print query range reverse_order right_outer_join select 
       select_all select_more set set_graph_aliases single_value size to_csv 
       transform union uniq unordered update where'
  
    # TODO: doc
    def self.[](*args)
      args = args.first if (args.size == 1)
      if args === true || args === false
        raise Error::InvalidFilter, "Did you mean to supply a hash?"
      end
      dataset[(Hash === args) ? args : primary_key_hash(args)]
    end
    
    # Returns the columns in the result set in their original order.
    #
    # See Dataset#columns for more information.
    def self.columns
      return @columns if @columns
      @columns = dataset.naked.columns or
      raise Error, "Could not fetch columns for #{self}"
      def_column_accessor(*@columns)
      @str_columns = nil
      @columns
    end
  
    # Returns the dataset associated with the Model class.
    def self.dataset
      @dataset || raise(Error, "No dataset associated with #{self}")
    end
  
    # Returns the database associated with the Model class.
    def self.db
      return @db if @db
      @db = self == Model ? DATABASES.first : superclass.db
      raise(Error, "No database associated with #{self}") unless @db
      @db
    end
    
    # Sets the database associated with the Model class.
    def self.db=(db)
      @db = db
      if @dataset
        set_dataset(db[table_name])
      end
    end
    
    # If a block is given, define a method on the dataset with the given argument name using
    # the given block as well as a method on the model that calls the
    # dataset method.
    #
    # If a block is not given, define a method on the model for each argument
    # that calls the dataset method of the same argument name.
    def self.def_dataset_method(*args, &block)
      raise(Error, "No arguments given") if args.empty?
      if block_given?
        raise(Error, "Defining a dataset method using a block requires only one argument") if args.length > 1
        dataset.meta_def(args.first, &block)
      end
      args.each{|arg| instance_eval("def #{arg}(*args, &block); dataset.#{arg}(*args, &block) end", __FILE__, __LINE__)}
    end
    
    # Deletes all records in the model's table.
    def self.delete_all
      dataset.delete
    end
  
    # Like delete_all, but invokes before_destroy and after_destroy hooks if used.
    def self.destroy_all
      dataset.destroy
    end
  
    # TODO: doc
    def self.fetch(*args)
      db.fetch(*args).set_model(self)
    end
  
    # Finds a single record according to the supplied filter, e.g.:
    #
    #   Ticket.find :author => 'Sharon' # => record
    def self.find(*args, &block)
      dataset.filter(*args, &block).first
    end
    
    # Like find but invokes create with given conditions when record does not
    # exists.
    def self.find_or_create(cond)
      find(cond) || create(cond)
    end
  
    # If possible, set the dataset for the model subclass as soon as it
    # is created.
    def self.inherited(subclass)
      begin
        if subclass.superclass == Model
          unless subclass.name.empty?
            subclass.set_dataset(Model.db[subclass.implicit_table_name])
          end
        elsif ds = subclass.superclass.instance_variable_get(:@dataset)
          subclass.set_dataset(ds.clone)
        end
      rescue StandardError
      end
    end
  
    # Returns the implicit table name for the model class.
    def self.implicit_table_name
      name.demodulize.underscore.pluralize.to_sym
    end
  
    # Serializes column with YAML or through marshalling.
    def self.serialize(*columns)
      format = columns.pop[:format] if Hash === columns.last
      format ||= :yaml
      
      @transform = columns.inject({}) do |m, c|
        m[c] = format
        m
      end
      @dataset.transform(@transform) if @dataset
    end
  
    # Sets the dataset associated with the Model class.
    # Also has the alias dataset=.
    def self.set_dataset(ds)
      @db = ds.db
      @dataset = ds
      @dataset.set_model(self)
      @dataset.extend(Associations::EagerLoading)
      @dataset.transform(@transform) if @transform
      begin
        @columns = nil
        columns
      rescue StandardError
      end
    end
    metaalias :dataset=, :set_dataset
  
    # Returns the columns as a list of frozen strings.
    def self.str_columns
      @str_columns ||= columns.map{|c| c.to_s.freeze}
    end
  
    # Defines a method that returns a filtered dataset.
    def self.subset(name, *args, &block)
      def_dataset_method(name){filter(*args, &block)}
    end
    
    # Add model methods that call dataset methods
    def_dataset_method *DATASET_METHODS

    ### Private Class Methods ###
    
    # Create the column accessors
    def self.def_column_accessor(*columns) # :nodoc:
      Thread.exclusive do
        columns.each do |column|
          im = instance_methods
          meth = "#{column}="
          define_method(column){self[column]} unless im.include?(column.to_s)
          unless im.include?(meth)
            define_method(meth) do |*v|
              len = v.length
              raise(ArgumentError, "wrong number of arguments (#{len} for 1)") unless len == 1
              self[column] = v.first 
            end
          end
        end
      end
    end
    metaprivate :def_column_accessor
  end
end
