module Sequel
  class Model
    # The setter methods (methods ending with =) that are never allowed
    # to be called automatically via set.
    RESTRICTED_SETTER_METHODS = %w"== === []= taguri= typecast_empty_string_to_nil= typecast_on_assignment= strict_param_setting= raise_on_save_failure= raise_on_typecast_failure="

    # The current cached associations.  A hash with the keys being the
    # association name symbols and the values being the associated object
    # or nil (many_to_one), or the array of associated objects (*_to_many).
    attr_reader :associations

    # The columns that have been updated.  This isn't completely accurate,
    # see Model#[]=.
    attr_reader :changed_columns

    # The hash of attribute values.  Keys are symbols with the names of the
    # underlying database columns.
    attr_reader :values

    class_attr_reader :columns, :dataset, :db, :primary_key, :str_columns
    class_attr_overridable :db_schema, :raise_on_save_failure, :raise_on_typecast_failure, :strict_param_setting, :typecast_empty_string_to_nil, :typecast_on_assignment
    remove_method :db_schema=
    
    # Creates new instance with values set to passed-in Hash.
    # If a block is given, yield the instance to the block unless
    # from_db is true.
    # This method runs the after_initialize hook after
    # it has optionally yielded itself to the block.
    #
    # Arguments:
    # * values - should be a hash with symbol keys, though
    #   string keys will work if from_db is false.
    # * from_db - should only be set by Model.load, forget it
    #   exists.
    def initialize(values = {}, from_db = false, &block)
      @associations = {}
      @changed_columns = []
      if from_db
        @new = false
        @values = values
      else
        @values = {}
        @new = true
        set(values)
        @changed_columns.clear 
        yield self if block
      end
      after_initialize
    end
    
    # Returns value of the column's attribute.
    def [](column)
      @values[column]
    end

    # Sets value of the column's attribute and marks the column as changed.
    # If the column already has the same value, this is a no-op.
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

    # class is defined in Object, but it is also a keyword,
    # and since a lot of instance methods call class methods,
    # the model makes it so you can use model instead of
    # self.class.
    alias_method :model, :class

    # Deletes and returns self.  Does not run destroy hooks.
    # Look into using destroy instead.
    def delete
      before_delete
      this.delete
      self
    end
    
    # Like delete but runs hooks before and after delete.
    # If before_destroy returns false, returns false without
    # deleting the object the the database. Otherwise, deletes
    # the item from the database and returns self.
    def destroy
      db.transaction do
        return save_failure(:destroy) if before_destroy == false
        delete
        after_destroy
      end
      self
    end
    
    # Enumerates through all attributes.
    #
    # Example:
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

    # Returns value for the :id attribute, even if the primary key is
    # not id. To get the primary key value, use #pk.
    def id
      @values[:id]
    end

    # Returns a string representation of the model instance including
    # the class name and values.
    def inspect
      "#<#{model.name} @values=#{@values.inspect}>"
    end

    # Returns attribute names as an array of symbols.
    def keys
      @values.keys
    end

    # Returns true if the current instance represents a new record.
    def new?
      @new
    end
    
    # Returns the primary key value identifying the model instance.
    # Raises an error if this model does not have a primary key.
    # If the model has a composite primary key, returns an array of values.
    def pk
      raise(Error, "No primary key is associated with this model") unless key = primary_key
      case key
      when Array
        key.collect{|k| @values[k]}
      else
        @values[key]
      end
    end
    
    # Returns a hash identifying the model instance.  It should be true that:
    # 
    #  Model[model_instance.pk_hash] === model_instance
    def pk_hash
      model.primary_key_hash(pk)
    end
    
    # Reloads attributes from database and returns self. Also clears all
    # cached association information.  Raises an Error if the record no longer
    # exists in the database.
    def refresh
      @values = this.first || raise(Error, "Record not found")
      @associations.clear
      self
    end
    alias_method :reload, :refresh

    # Creates or updates the record, after making sure the record
    # is valid.  If the record is not valid, or before_save,
    # before_create (if new?), or before_update (if !new?) return
    # false, returns nil unless raise_on_save_failure is true (if it
    # is true, it raises an error).
    # Otherwise, returns self. You can provide an optional list of
    # columns to update, in which case it only updates those columns.
    def save(*columns)
      return save_failure(:save) unless valid?
      save!(*columns)
    end

    # Creates or updates the record, without attempting to validate
    # it first. You can provide an optional list of columns to update,
    # in which case it only updates those columns.
    # If before_save, before_create (if new?), or before_update
    # (if !new?) return false, returns nil unless raise_on_save_failure
    # is true (if it is true, it raises an error).  Otherwise, returns self.
    def save!(*columns)
      opts = columns.extract_options!
      return save_failure(:save) if before_save == false
      if @new
        return save_failure(:create) if before_create == false
        ds = model.dataset
        if ds.respond_to?(:insert_select) and h = ds.insert_select(@values)
          @values = h
          @this = nil
        else
          iid = ds.insert(@values)
          # if we have a regular primary key and it's not set in @values,
          # we assume it's the last inserted id
          if (pk = primary_key) && !(Array === pk) && !@values[pk]
            @values[pk] = iid
          end
          if pk
            @this = nil # remove memoized this dataset
            refresh
          end
        end
        @new = false
        after_create
      else
        return save_failure(:update) if before_update == false
        if columns.empty?
          vals = opts[:changed] ? @values.reject{|k,v| !@changed_columns.include?(k)} : @values
          this.update(vals)
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
    # chanaged.  If no columns have been changed, returns nil.  If unable to
    # save, returns false unless raise_on_save_failure is true.
    def save_changes
      save(:changed=>true) || false unless @changed_columns.empty?
    end

    # Updates the instance with the supplied values with support for virtual
    # attributes, raising an exception if a value is used that doesn't have
    # a setter method (or ignoring it if strict_param_setting = false).
    # Does not save the record.
    #
    # If no columns have been set for this model (very unlikely), assume symbol
    # keys are valid column names, and assign the column value based on that.
    def set(hash)
      set_restricted(hash, nil, nil)
    end
    alias_method :set_with_params, :set

    # Set all values using the entries in the hash, ignoring any setting of
    # allowed_columns or restricted columns in the model.
    def set_all(hash)
      set_restricted(hash, false, false)
    end

    # Set all values using the entries in the hash, except for the keys
    # given in except.
    def set_except(hash, *except)
      set_restricted(hash, false, except.flatten)
    end

    # Set the values using the entries in the hash, only if the key
    # is included in only.
    def set_only(hash, *only)
      set_restricted(hash, only.flatten, false)
    end

    # Sets the value attributes without saving the record.  Returns
    # the values changed.  Raises an error if the keys are not symbols
    # or strings or a string key was passed that was not a valid column.
    # This is a low level method that does not respect virtual attributes.  It
    # should probably be avoided.  Look into using set instead.
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
          raise(Error, "all string keys must be a valid columns") unless s.include?(k)
          k.to_sym
        else
          raise(Error, "Only symbols and strings allows as keys")
        end
        m[k] = v
        m
      end
      vals.each {|k, v| @values[k] = v}
      vals
    end

    # Returns (naked) dataset that should return only this instance.
    def this
      @this ||= dataset.filter(pk_hash).limit(1).naked
    end
    
    # Runs set with the passed hash and runs save_changes (which runs any callback methods).
    def update(hash)
      update_restricted(hash, nil, nil)
    end
    alias_method :update_with_params, :update

    # Update all values using the entries in the hash, ignoring any setting of
    # allowed_columns or restricted columns in the model.
    def update_all(hash)
      update_restricted(hash, false, false)
    end

    # Update all values using the entries in the hash, except for the keys
    # given in except.
    def update_except(hash, *except)
      update_restricted(hash, false, except.flatten)
    end

    # Update the values using the entries in the hash, only if the key
    # is included in only.
    def update_only(hash, *only)
      update_restricted(hash, only.flatten, false)
    end

    # Sets the values attributes with set_values and then updates
    # the record in the database using those values.  This is a
    # low level method that does not run the usual save callbacks.
    # It should probably be avoided.  Look into using update_with_params instead.
    def update_values(values)
      before_update_values
      this.update(set_values(values))
    end
    
    private

    # Backbone behind association_dataset
    def _dataset(opts)
      raise(Sequel::Error, 'model object does not have a primary key') if opts.dataset_need_primary_key? && !pk
      ds = send(opts._dataset_method)
      opts[:extend].each{|m| ds.extend(m)}
      ds = ds.select(*opts.select) if opts.select
      ds = ds.order(*opts[:order]) if opts[:order]
      ds = ds.limit(*opts[:limit]) if opts[:limit]
      ds = ds.eager(*opts[:eager]) if opts[:eager]
      ds = ds.eager_graph(opts[:eager_graph]) if opts[:eager_graph] && opts.eager_graph_lazy_dataset?
      ds = send(opts.dataset_helper_method, ds) if opts[:block]
      ds
    end

    # Add the given associated object to the given association
    def add_associated_object(opts, o)
      raise(Sequel::Error, 'model object does not have a primary key') unless pk
      raise(Sequel::Error, 'associated object does not have a primary key') if opts.need_associated_primary_key? && !o.pk
      return if run_association_callbacks(opts, :before_add, o) == false
      send(opts._add_method, o)
      @associations[opts[:name]].push(o) if @associations.include?(opts[:name])
      add_reciprocal_object(opts, o)
      run_association_callbacks(opts, :after_add, o)
      o
    end

    # Add/Set the current object to/as the given object's reciprocal association.
    def add_reciprocal_object(opts, o)
      return unless reciprocal = opts.reciprocal
      case opts[:type]
      when :many_to_many, :many_to_one
        if array = o.associations[reciprocal] and !array.include?(self)
          array.push(self)
        end
      when :one_to_many
        o.associations[reciprocal] = self
      end
    end

    # Load the associated objects using the dataset
    def load_associated_objects(opts, reload=false)
      name = opts[:name]
      if @associations.include?(name) and !reload
        @associations[name]
      else
        objs = if opts.single_associated_object?
          if !opts[:key]
            send(opts.dataset_method).all.first
          elsif send(opts[:key])
            send(opts.dataset_method).first
          end
        else
          objs = send(opts.dataset_method).all
        end
        run_association_callbacks(opts, :after_load, objs)
        # Only one_to_many associations should set the reciprocal object
        objs.each{|o| add_reciprocal_object(opts, o)} if opts.set_reciprocal_to_self?
        @associations[name] = objs
      end
    end

    # Remove all associated objects from the given association
    def remove_all_associated_objects(opts)
      raise(Sequel::Error, 'model object does not have a primary key') unless pk
      send(opts._remove_all_method)
      ret = @associations[opts[:name]].each{|o| remove_reciprocal_object(opts, o)} if @associations.include?(opts[:name])
      @associations[opts[:name]] = []
      ret
    end

    # Remove the given associated object from the given association
    def remove_associated_object(opts, o)
      raise(Sequel::Error, 'model object does not have a primary key') unless pk
      raise(Sequel::Error, 'associated object does not have a primary key') if opts.need_associated_primary_key? && !o.pk
      return if run_association_callbacks(opts, :before_remove, o) == false
      send(opts._remove_method, o)
      @associations[opts[:name]].delete_if{|x| o === x} if @associations.include?(opts[:name])
      remove_reciprocal_object(opts, o)
      run_association_callbacks(opts, :after_remove, o)
      o
    end

    # Remove/unset the current object from/as the given object's reciprocal association.
    def remove_reciprocal_object(opts, o)
      return unless reciprocal = opts.reciprocal
      case opts[:type]
      when :many_to_many, :many_to_one
        if array = o.associations[reciprocal]
          array.delete_if{|x| self === x}
        end
      when :one_to_many
        o.associations[reciprocal] = nil
      end
    end

    # Run the callback for the association with the object.
    def run_association_callbacks(reflection, callback_type, object)
      raise_error = raise_on_save_failure
      raise_error = true if reflection[:type] == :many_to_one
      stop_on_false = true if [:before_add, :before_remove].include?(callback_type)
      reflection[callback_type].each do |cb|
        res = case cb
        when Symbol
          send(cb, object)
        when Proc
          cb.call(self, object)
        else
          raise Error, "callbacks should either be Procs or Symbols"
        end
        if res == false and stop_on_false
          save_failure("modify association for", raise_error)
          return false
        end
      end
    end

    # Raise an error if raise_on_save_failure is true
    def save_failure(action, raise_error = nil)
      raise_error = raise_on_save_failure if raise_error.nil?
      raise(Error, "unable to #{action} record") if raise_error
    end

    # Set the columns, filtered by the only and except arrays.
    def set_restricted(hash, only, except)
      columns_not_set = model.instance_variable_get(:@columns).blank?
      meths = setter_methods(only, except)
      strict = strict_param_setting
      hash.each do |k,v|
        m = "#{k}="
        if meths.include?(m)
          send(m, v)
        elsif columns_not_set && (Symbol === k)
          self[k] = v
        elsif strict
          raise Error, "method #{m} doesn't exist or access is restricted to it"
        end
      end
      self
    end

    # Returns all methods that can be used for attribute
    # assignment (those that end with =), modified by the only
    # and except arguments:
    #
    # * only
    #   * false - Don't modify the results
    #   * nil - if the model has allowed_columns, use only these, otherwise, don't modify
    #   * Array - allow only the given methods to be used
    # * except
    #   * false - Don't modify the results
    #   * nil - if the model has restricted_columns, remove these, otherwise, don't modify
    #   * Array - remove the given methods
    #
    # only takes precedence over except, and if only is not used, certain methods are always
    # restricted (RESTRICTED_SETTER_METHODS).  The primary key is restricted by default as
    # well, see Model.unrestrict_primary_key to change this.
    def setter_methods(only, except)
      only = only.nil? ? model.allowed_columns : only
      except = except.nil? ? model.restricted_columns : except
      if only
        only.map{|x| "#{x}="}
      else
        meths = methods.collect{|x| x.to_s}.grep(/=\z/) - RESTRICTED_SETTER_METHODS
        meths -= Array(primary_key).map{|x| "#{x}="} if primary_key && model.restrict_primary_key?
        meths -= except.map{|x| "#{x}="} if except
        meths
      end
    end

    # Typecast the value to the column's type if typecasting.  Calls the database's
    # typecast_value method, so database adapters can override/augment the handling
    # for database specific column types.
    def typecast_value(column, value)
      return value unless typecast_on_assignment && db_schema && (col_schema = db_schema[column]) && !model.serialized?(column)
      value = nil if value == '' and typecast_empty_string_to_nil and col_schema[:type] and ![:string, :blob].include?(col_schema[:type])
      raise(Error::InvalidValue, "nil/NULL is not allowed for the #{column} column") if raise_on_typecast_failure && value.nil? && (col_schema[:allow_null] == false)
      begin
        model.db.typecast_value(col_schema[:type], value)
      rescue Error::InvalidValue
        if raise_on_typecast_failure
          raise
        else
          value
        end
      end
    end

    # Call uniq! on the given array. This is used by the :uniq option,
    # and is an actual method for memory reasons.
    def array_uniq!(a)
      a.uniq!
    end

    # Set the columns, filtered by the only and except arrays.
    def update_restricted(hash, only, except)
      set_restricted(hash, only, except)
      save_changes
    end
  end
end
