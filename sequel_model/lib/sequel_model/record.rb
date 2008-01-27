module Sequel
  class Model
    attr_reader :values
    attr_reader :changed_columns

    # Returns value of attribute.
    def [](column)
      @values[column]
    end
    # Sets value of attribute and marks the column as changed.
    def []=(column, value)
      @values[column] = value
      @changed_columns << column unless @changed_columns.include?(column)
    end

    # Enumerates through all attributes.
    #
    # === Example:
    #   Ticket.find(7).each { |k, v| puts "#{k} => #{v}" }
    def each(&block)
      @values.each(&block)
    end
    # Returns attribute names.
    def keys
      @values.keys
    end

    # Returns value for <tt>:id</tt> attribute.
    def id
      @values[:id]
    end

    # Compares model instances by values.
    def ==(obj)
      (obj.class == model) && (obj.values == @values)
    end

    # Compares model instances by pkey.
    def ===(obj)
      (obj.class == model) && (obj.pk == pk)
    end

    # Returns key for primary key.
    def self.primary_key
      :id
    end
    
    # Returns a string representation of the primary key
    # Example:
    #   primary_key [:title, :category]
    #   title_category
    def self.primary_key_string
      if self.primary_key.class == Array
        self.primary_key.join("_")
      else
        self.primary_key.to_s
      end
    end
    
    # Returns primary key attribute hash.
    def self.primary_key_hash(value)
      {:id => value}
    end
    
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
    
    def self.no_primary_key #:nodoc:
      meta_def(:primary_key) {nil}
      meta_def(:primary_key_hash) {|v| raise Error, "#{self} does not have a primary key"}
      class_def(:this)      {raise Error, "No primary key is associated with this model"}
      class_def(:pk)        {raise Error, "No primary key is associated with this model"}
      class_def(:pk_hash)   {raise Error, "No primary key is associated with this model"}
      class_def(:cache_key) {raise Error, "No primary key is associated with this model"}
    end
    
    # Creates new instance with values set to passed-in Hash ensuring that
    # new? returns true.
    def self.create(values = {}, &block)
      db.transaction do
        obj = new(values, &block)
        obj.save
        obj
      end
    end
    
    # Updates the instance with the supplied values with support for virtual
    # attributes, ignoring any values for which no setter method is available.
    def update_with_params(values)
      c = columns
      values.each do |k, v| m = :"#{k}="
        send(m, v) if c.include?(k) || respond_to?(m)
      end
      save_changes
    end
    alias_method :update_with, :update_with_params

    class << self
      def create_with_params(params)
        record = new
        record.update_with_params(params)
        record
      end
      alias_method :create_with, :create_with_params
    end
    
    # Returns (naked) dataset bound to current instance.
    def this
      @this ||= self.class.dataset.filter(:id => @values[:id]).limit(1).naked
    end
    
    # Returns a key unique to the underlying record for caching
    def cache_key
      pk = @values[:id] || (raise Error, 'no primary key for this record')
      @cache_key ||= "#{self.class}:#{pk}"
    end

    # Returns primary key column(s) for object's Model class.
    def primary_key
      @primary_key ||= self.class.primary_key
    end
    
    # Returns the primary key value identifying the model instance. Stock implementation.
    def pk
      @pk ||= @values[:id]
    end
    
    # Returns a hash identifying the model instance. Stock implementation.
    def pk_hash
      @pk_hash ||= {:id => @values[:id]}
    end
    
    # Creates new instance with values set to passed-in Hash.
    #
    # This method guesses whether the record exists when
    # <tt>new_record</tt> is set to false.
    def initialize(values = nil, from_db = false, &block)
      @changed_columns = []
      unless from_db
        @values = {}
        if values
          values.each do |k, v| m = :"#{k}="
            if respond_to?(m)
              send(m, v)
              values.delete(k)
            end
          end
          @values.merge!(values)
        end
      else
        @values = values || {}
      end

      k = primary_key
      if from_db
        @new = k == nil
      else
        # if there's no primary key for the model class, or
        # @values doesn't contain a primary key value, then 
        # we regard this instance as new.
        @new = (k == nil) || (!(Array === k) && !@values[k])
      end
      
      block[self] if block
      after_initialize
    end
    
    def self.load(values)
      new(values, true)
    end
    
    # Returns true if the current instance represents a new record.
    def new?
      @new
    end
    alias :new_record? :new?
    
    # Returns true when current instance exists, false otherwise.
    def exists?
      this.count > 0
    end
    
    # Creates or updates the associated record. This method can also
    # accept a list of specific columns to update.
    def save(*columns)
      before_save
      if @new
        before_create
        iid = model.dataset.insert(@values)
        # if we have a regular primary key and it's not set in @values,
        # we assume it's the last inserted id
        if (pk = primary_key) && !(Array === pk) && !@values[pk]
          @values[pk] = iid
        end
        if pk
          @this = nil # remove memoized this dataset
          refresh
        end
        @new = false
        after_create
      else
        before_update
        if columns.empty?
          this.update(@values)
          @changed_columns = []
        else # update only the specified columns
          this.update(@values.reject {|k, v| !columns.include?(k)})
          @changed_columns.reject! {|c| columns.include?(c)}
        end
        after_update
      end
      after_save
      self
    end
    
    # Saves only changed columns or does nothing if no columns are marked as 
    # chanaged.
    def save_changes
      save(*@changed_columns) unless @changed_columns.empty?
    end

    # Updates and saves values to database from the passed-in Hash.
    def set(values)
      this.update(values)
      values.each {|k, v| @values[k] = v}
    end
    alias_method :update, :set
    
    # Reloads values from database and returns self.
    def refresh
      @values = this.first || raise(Error, "Record not found")
      self
    end
    alias_method :reload, :refresh

    # Like delete but runs hooks before and after delete.
    def destroy
      db.transaction do
        before_destroy
        delete
        after_destroy
      end
    end
    
    # Deletes and returns self.
    def delete
      this.delete
      self
    end
    
    ATTR_RE = /^([a-zA-Z_]\w*)(=)?$/.freeze

    def method_missing(m, *args) #:nodoc:
      if m.to_s =~ ATTR_RE
        att = $1.to_sym
        write = $2 == '='
        
        # check whether the column is legal
        unless columns.include?(att)
          # if read accessor and a value exists for the column, we return it
          if !write && @values.has_key?(att)
            return @values[att]
          end
          
          # otherwise, raise an error
          raise Error, "Invalid column (#{att.inspect}) for #{self}"
        end

        # define the column accessor
        Thread.exclusive do
          if write
            model.class_def(m) {|v| self[att] = v}
          else
            model.class_def(m) {self[att]}
          end
        end
        
        # call the accessor
        respond_to?(m) ? send(m, *args) : super(m, *args)
      else
        super(m, *args)
      end
    end
  end
end