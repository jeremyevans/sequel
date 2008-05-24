module Sequel
  class Model
    alias_method :model, :class

    attr_reader :changed_columns, :values
    attr_writer :typecast_on_assignment

    class_attr_reader :columns, :dataset, :db, :primary_key, :str_columns
    
    # Creates new instance with values set to passed-in Hash.
    #
    # This method guesses whether the record exists when
    # <tt>new_record</tt> is set to false.
    def initialize(values = nil, from_db = false, &block)
      values ||=  {}
      @changed_columns = []
      @typecast_on_assignment = model.typecast_on_assignment
      @db_schema = model.db_schema
      if from_db
        @new = false
        @values = values
      else
        @values = {}
        @new = true
        set_with_params(values)
      end
      @changed_columns.clear 
      
      yield self if block
      after_initialize
    end
    
    # Returns value of attribute.
    def [](column)
      @values[column]
    end

    # Sets value of attribute and marks the column as changed.
    def []=(column, value)
      # If it is new, it doesn't have a value yet, so we should
      # definitely set the new value.
      # If the column isn't in @values, we can't assume it is
      # NULL in the database, so assume it has changed.
      if new? || !@values.include?(column) || value != @values[column]
        @changed_columns << column unless @changed_columns.include?(column)
        @values[column] = typecast_value(column, value)
      end
    end

    # Compares model instances by values.
    def ==(obj)
      (obj.class == model) && (obj.values == @values)
    end
    alias_method :eql?, :"=="

    # If pk is not nil, true only if the objects have the same class and pk.
    # If pk is nil, false.
    def ===(obj)
      pk.nil? ? false : (obj.class == model) && (obj.pk == pk)
    end

    # Deletes and returns self.  Does not run callbacks.
    # Look into using destroy instead.
    def delete
      before_delete
      this.delete
      self
    end
    
    # Like delete but runs hooks before and after delete.
    def destroy
      db.transaction do
        return false if before_destroy == false
        delete
        after_destroy
      end
      self
    end
    
    # Enumerates through all attributes.
    #
    # === Example:
    #   Ticket.find(7).each { |k, v| puts "#{k} => #{v}" }
    def each(&block)
      @values.each(&block)
    end

    # Returns true when current instance exists, false otherwise.
    def exists?
      this.count > 0
    end
    
    # Unique for objects with the same class and pk (if pk is not nil), or
    # the same class and values (if pk is nil).
    def hash
      [model, pk.nil? ? @values.sort_by{|k,v| k.to_s} : pk].hash
    end

    # Returns value for <tt>:id</tt> attribute.
    def id
      @values[:id]
    end

    # Returns a string representation of the model instance including
    # the class name and values.
    def inspect
      "#<%s @values=%s>" % [model.name, @values.inspect]
    end

    # Returns attribute names.
    def keys
      @values.keys
    end

    # Returns true if the current instance represents a new record.
    def new?
      @new
    end
    
    # Returns the primary key value identifying the model instance. If the
    # model's primary key is changed (using #set_primary_key or #no_primary_key)
    # this method is redefined accordingly.
    def pk
      raise(Error, "No primary key is associated with this model") unless key = primary_key
      case key
      when Array
        key.collect{|k| @values[k]}
      else
        @values[key]
      end
    end
    
    # Returns a hash identifying the model instance. Stock implementation.
    def pk_hash
      model.primary_key_hash(pk)
    end
    
    # Reloads values from database and returns self.
    def refresh
      @values = this.first || raise(Error, "Record not found")
      model.all_association_reflections.each do |r|
        instance_variable_set("@#{r[:name]}", nil)
      end
      self
    end
    alias_method :reload, :refresh

    # Creates or updates the record, after making sure the record
    # is valid.  If the record is not valid, returns false.
    def save(*columns)
      return false unless valid?
      save!(*columns)
    end

    # Creates or updates the record, without attempting to validate
    # it first. You can provide an optional list of columns to update,
    # in which case it only updates those columns
    def save!(*columns)
      return false if before_save == false
      if @new
        return false if before_create == false
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
        return false if before_update == false
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

    # Sets the value attributes without saving the record.  Returns
    # the values changed.  Raises an error if the keys are not symbols
    # or strings or a string key was passed that was not a valid column.
    # This is a low level method that does not respect virtual attributes.  It
    # should probably be avoided.  Look into using set_with_params instead.
    def set_values(values)
      s = str_columns
      vals = values.inject({}) do |m, kv| 
        k, v = kv
        k = case k
        when Symbol
          k
        when String
          # Prevent denial of service via memory exhaustion by only 
          # calling to_sym if the symbol already exists.
          raise(::Sequel::Error, "all string keys must be a valid columns") unless s.include?(k)
          k.to_sym
        else
          raise(::Sequel::Error, "Only symbols and strings allows as keys")
        end
        m[k] = v
        m
      end
      vals.each {|k, v| @values[k] = v}
      vals
    end

    # Updates the instance with the supplied values with support for virtual
    # attributes, ignoring any values for which no setter method is available.
    # Does not save the record.
    #
    # If no columns have been set for this model (very unlikely), assume symbol
    # keys are valid column names, and assign the column value based on that.
    def set_with_params(hash)
      columns_not_set = !model.instance_variable_get(:@columns)
      meths = setter_methods
      hash.each do |k,v|
        m = "#{k}="
        if meths.include?(m)
          send(m, v)
        elsif columns_not_set && (Symbol === k)
          self[k] = v
        end
      end
    end

    # Returns (naked) dataset bound to current instance.
    def this
      @this ||= dataset.filter(pk_hash).limit(1).naked
    end
    
    # Sets the values attributes with set_values and then updates
    # the record in the database using those values.  This is a
    # low level method that does not run the usual save callbacks.
    # It should probably be avoided.  Look into using update_with_params instead.
    def update_values(values)
      before_update_values
      this.update(set_values(values))
    end
    
    # Runs set_with_params and saves the changes (which runs any callback methods).
    def update_with_params(values)
      set_with_params(values)
      save_changes
    end

    private
      # Returns all methods that can be used for attribute
      # assignment (those that end with =)
      def setter_methods
        methods.grep(/=\z/)
      end

      # Typecast the value to the column's type
      def typecast_value(column, value)
        return value unless @typecast_on_assignment && @db_schema && (col_schema = @db_schema[column])
        if value.nil?
          raise(Sequel::Error, "nil/NULL is not allowed for the #{column} column") if col_schema[:allow_null] == false
          return nil
        end
        case col_schema[:type]
        when :integer
          Integer(value)
        when :string
          value.to_s
        when :float
          Float(value)
        when :boolean
          case value
          when false, 0, "0", /\Af(alse)?\z/i
            false
          else
            value.blank? ? nil : true
          end
        when :date
          case value
          when Date
            value
          when DateTime, Time
            Date.new(value.year, value.month, value.day)
          when String
            value.to_date
          else
            raise ArgumentError, "invalid value for Date: #{value.inspect}"
          end
        when :datetime
          case value
          when DateTime
            value
          when Date
            DateTime.new(value.year, value.month, value.day)
          when Time
            DateTime.new(value.year, value.month, value.day, value.hour, value.min, value.sec)
          when String
            value.to_datetime
          else
            raise ArgumentError, "invalid value for DateTime: #{value.inspect}"
          end
        else
          value
        end
      end
  end
end
