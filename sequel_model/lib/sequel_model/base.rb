module Sequel
  class Model
    # Returns the database associated with the Model class.
    def self.db
      @db ||= (superclass != Object) && superclass.db or
      raise Error, "No database associated with #{self}"
    end
    
    # Sets the database associated with the Model class.
    def self.db=(db)
      @db = db
      if @dataset
        set_dataset(db[table_name])
      end
    end
    
    # Called when a database is opened in order to automatically associate the
    # first opened database with model classes.
    def self.database_opened(db)
      @db = db if (self == Model) && !@db
    end
    
    # Returns the implicit table name for the model class.
    def self.implicit_table_name
      name.demodulize.underscore.pluralize.to_sym
    end

    # Returns the dataset associated with the Model class.
    def self.dataset
      unless @dataset
        if ds = super_dataset
          set_dataset(ds.clone)
        elsif !name.empty?
          set_dataset(db[implicit_table_name])
        else
          raise Error, "No dataset associated with #{self}"
        end
      end
      @dataset
    end
    
    # def self.dataset
    #   @dataset ||= super_dataset ||
    #     (!(n = name).empty? && db[n.underscore.pluralize.to_sym]) ||
    #     (raise Error, "No dataset associated with #{self}")
    # end
    
    def self.super_dataset # :nodoc:
      superclass.dataset if (superclass != Sequel::Model) && superclass.respond_to?(:dataset)
    end
    
    # Returns the columns in the result set in their original order.
    #
    # See Dataset#columns for more information.
    def self.columns
      @columns ||= dataset.columns or
      raise Error, "Could not fetch columns for #{self}"
    end

    # Sets the dataset associated with the Model class.
    def self.set_dataset(ds)
      @db = ds.db
      @dataset = ds
      @dataset.set_model(self)
      @dataset.extend(Associations::EagerLoading)
      @dataset.transform(@transform) if @transform
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
  def self.Model(source)
    @models ||= {}
    @models[source] ||= Class.new(Sequel::Model) do
      meta_def(:inherited) do |c|
        c.set_dataset(source.is_a?(Dataset) ? source : c.db[source])
      end
    end
  end
  
end
