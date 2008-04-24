module Sequel
  class Model
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

    # Returns the database associated with the Model class.
    def self.db
      return @db if @db
      @db = self == Model ? ::Sequel::DATABASES.first : superclass.db
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
    
    # Returns the implicit table name for the model class.
    def self.implicit_table_name
      name.demodulize.underscore.pluralize.to_sym
    end

    # Returns the dataset associated with the Model class.
    def self.dataset
      @dataset || raise(Error, "No dataset associated with #{self}")
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

    # Returns the columns as a list of frozen strings.
    def self.str_columns
      @str_columns ||= columns.map{|c| c.to_s.freeze}
    end

    # Sets the dataset associated with the Model class.
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
    class << self; alias :dataset= :set_dataset; end

    class << self
      private
        def def_column_accessor(*columns)
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
    end
    
    # Returns the database assoiated with the object's Model class.
    def db
      @db ||= model.db
    end

    # Returns the dataset assoiated with the object's Model class.
    #
    # See Dataset for more information.
    def dataset
      model.dataset
    end
    
    # Returns the columns associated with the object's Model class.
    def columns
      model.columns
    end

    # Returns the str_columns associated with the object's Model class.
    def str_columns
      model.str_columns
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
  end

  # Lets you create a Model class with its table name already set or reopen
  # an existing Model.
  #
  # Makes given dataset inherited.
  #
  # === Example:
  #   class Comment < Sequel::Model(:something)
  #     table_name # => :something
  #
  #     # ...
  #
  #   end
  @models = {}
  def self.Model(source)
    return @models[source] if @models[source]
    klass = Class.new(Sequel::Model)
    klass.set_dataset(source.is_a?(Dataset) ? source : Model.db[source])
    @models[source] = klass
  end
end
