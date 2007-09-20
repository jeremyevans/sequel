module Sequel
  class Model
    attr_reader :values

    def self.primary_key; :id; end
    
    def self.set_primary_key(key)
      # if k is nil, we go to no_primary_key
      return no_primary_key unless key
      
      # redefine primary_key
      meta_def(:primary_key) {key}
      
      if key.is_a?(Array) # composite key
        class_def(:this) do
          @this ||= self.class.dataset.filter( \
            @values.reject {|k, v| !key.include?(k)} \
          ).naked
        end
      else # regular key
        class_def(:this) do
          @this ||= self.class.dataset.filter(key => @values[key]).naked
        end
      end
    end
    
    def self.no_primary_key
      meta_def(:primary_key) {nil}
      class_def(:this) {raise SequelError, "No primary key is associated with this model"}
    end
    
    def self.create(values = {})
      db.transaction do
        obj = new(values, true)
        obj.save
        obj
      end
    end
    
    def this
      @this ||= self.class.dataset.filter(:id => @values[:id]).naked
    end
    
    # instance method
    def primary_key
      @primary_key ||= self.class.primary_key
    end
    
    def pkey
      @pkey ||= @values[primary_key]
    end
    
    def initialize(values = {}, new = false)
      @values = values
      @new = new
      if !new # determine if it's a new record
        pk = primary_key
        @new = (pk == nil) || (!(Array === pk) && !@values[pk])
      end
    end
    
    def new?
      @new
    end
    
    def exists?
      this.count > 0
    end
    
    def save
      run_hooks(:before_save)
      if @new
        run_hooks(:before_create)
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
        run_hooks(:after_create)
      else
        run_hooks(:before_update)
        this.update(@values)
        run_hooks(:after_update)
      end
      run_hooks(:after_save)
      @new = false
      self
    end

    def set(values)
      this.update(values)
      @values.merge!(values)
    end
    
    def refresh
      @values = this.first || raise(SequelError, "Record not found")
      self
    end

    def destroy
      db.transaction do
        run_hooks(:before_destroy)
        delete
        run_hooks(:after_destroy)
      end
    end
    
    def delete
      this.delete
      self
    end
    
    ATTR_RE = /^([a-zA-Z_]\w*)(=)?$/.freeze

    def method_missing(m, *args)
      if m.to_s =~ ATTR_RE
        att = $1.to_sym
        write = $2 == '='
        
        # check wether the column is legal
        unless columns.include?(att)
          raise SequelError, "Invalid column (#{att.inspect}) for #{self}"
        end

        # define the column accessor
        Thread.exclusive do
          if write
            model.class_def(m) {|v| @values[att] = v}
          else
            model.class_def(m) {@values[att]}
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