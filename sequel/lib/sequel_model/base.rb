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
  
    # Creates new instance with values set to passed-in Hash, saves it
    # (running any callbacks), and returns the instance.
    def self.create(values = {}, &block)
      obj = new(values, &block)
      obj.save
      obj
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
  
    # Initializes a model instance as an existing record. This constructor is
    # used by Sequel to initialize model instances when fetching records.
    def self.load(values)
      new(values, true)
    end

    def self.no_primary_key #:nodoc:
      meta_def(:primary_key) {nil}
      meta_def(:primary_key_hash) {|v| raise Error, "#{self} does not have a primary key"}
      class_def(:this)      {raise Error, "No primary key is associated with this model"}
      class_def(:pk)        {raise Error, "No primary key is associated with this model"}
      class_def(:pk_hash)   {raise Error, "No primary key is associated with this model"}
      class_def(:cache_key) {raise Error, "No primary key is associated with this model"}
    end

    # Returns key for primary key.
    def self.primary_key
      :id
    end

    # Returns primary key attribute hash.
    def self.primary_key_hash(value)
      {:id => value}
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
  
    # Sets primary key, regular and composite are possible.
    #
    # == Example:
    #   class Tagging < Sequel::Model
    #     # composite key
    #     set_primary_key :taggable_id, :tag_id
    #   end
    #
    #   class Person < Sequel::Model
    #     # regular key
    #     set_primary_key :person_id
    #   end
    #
    # <i>You can even set it to nil!</i>
    def self.set_primary_key(*key)
      # if k is nil, we go to no_primary_key
      if key.empty? || (key.size == 1 && key.first == nil)
        return no_primary_key
      end

      # backwards compat
      key = (key.length == 1) ? key[0] : key.flatten

      # redefine primary_key
      meta_def(:primary_key) {key}

      unless key.is_a? Array # regular primary key
        class_def(:this) do
          @this ||= dataset.filter(key => @values[key]).limit(1).naked
        end
        class_def(:pk) do
          @pk ||= @values[key]
        end
        class_def(:pk_hash) do
          @pk ||= {key => @values[key]}
        end
        class_def(:cache_key) do
          pk = @values[key] || (raise Error, 'no primary key for this record')
          @cache_key ||= "#{self.class}:#{pk}"
        end
        meta_def(:primary_key_hash) do |v|
          {key => v}
        end
      else # composite key
        exp_list = key.map {|k| "#{k.inspect} => @values[#{k.inspect}]"}
        block = eval("proc {@this ||= self.class.dataset.filter(#{exp_list.join(',')}).limit(1).naked}")
        class_def(:this, &block)

        exp_list = key.map {|k| "@values[#{k.inspect}]"}
        block = eval("proc {@pk ||= [#{exp_list.join(',')}]}")
        class_def(:pk, &block)

        exp_list = key.map {|k| "#{k.inspect} => @values[#{k.inspect}]"}
        block = eval("proc {@this ||= {#{exp_list.join(',')}}}")
        class_def(:pk_hash, &block)

        exp_list = key.map {|k| '#{@values[%s]}' % k.inspect}.join(',')
        block = eval('proc {@cache_key ||= "#{self.class}:%s"}' % exp_list)
        class_def(:cache_key, &block)

        meta_def(:primary_key_hash) do |v|
          key.inject({}) {|m, i| m[i] = v.shift; m}
        end
      end
    end

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
