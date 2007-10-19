module Sequel
  # == Cheatsheet:
  #   class Item < Sequel::Model(:items)
  #     set_schema do
  #       primary_key :id
  #       text :name, :unique => true, :null => false
  #       boolean :active, :default => true
  #       integer :grade
  #
  #       index :grade
  #     end
  #   end
  #
  #   Item.create_table unless Item.table_exists?
  #   Item.create_table!
  #
  #   i = Item.create(:name => 'Shoes', :grade => 0)
  #
  #   Item[1].grade #=> 0
  #
  #   i.set(:grade => 2)
  #   i.grade # => 2
  #
  #   Item[:name => 'Shoes'].grade # => 2
  #
  #   i.grade = 4
  #   Item[1].grade # => 2
  #   i.save
  #   Item[1].grade # => 4
  #
  # == Subsets
  # Subsets are filter mapped to class methods:
  #
  #   class Ticket < Sequel::Model(:tickets)
  #
  #     subset(:pending) { finished_at == nil }
  #     subset(:closed)  { finished_at != nil }
  #
  #     # ...
  #
  #   end
  #
  # Now you can do:
  #
  #   Ticket.pending.each { |ticket| puts ticket.caption }
  #
  # == Advanced filtering methods (or dataset magic)
  # One of the cool features of Sequel::Model is that it acts as a proxy to
  # the underlying dataset, so you can invoke methods on the class instead of
  # on the dataset:
  #
  #   Customer.filter(:name =~ 'Roberts')
  #
  # In the prevailing style of implementing models (which is actually very
  # similar to ActiveRecord models) table-wide operations are defined as
  # class methods:
  #
  #   class Node < Sequel::Model(:nodes)
  #     def self.subtree(path)
  #       filter(:path => Regexp.new("^#{path}(/.+)?$"))
  #     end
  #     def self.alarms
  #       filter {:kind => ALARM}
  #     end
  #     def self.recalculate
  #       exclude(:expression => nil).each {|n| n.calculate}
  #     end
  #   end
  #
  # The recalculate class method calls the exclude method. The exclude
  # call is proxied to the underlying dataset, which lets you call each
  # method separately:
  #
  #   Node.subtree('/test')
  #   Node.alarms
  #   Node.recalculate
  #
  # ... but this will raise a NoMethodError:
  #
  #   Node.subtree('/test').alarms.recalculate
  #
  # It turns out the solution is very simple - instead of defining class
  # methods, define dataset methods:
  #
  #   class Node < Sequel::Model(:nodes)
  #     def dataset.subtree(path)
  #       filter(:path => Regexp.new("^#{path}(/.+)?$"))
  #     end
  #     def dataset.alarms
  #       filter {:kind => ALARM}
  #     end
  #     def dataset.recalculate
  #       exclude(:expression => nil).each {|n| n.calculate}
  #     end
  #   end
  #
  # Now you can mix all of these methods any way you like:
  #
  #   Node.filter {:stamp < Time.now < 3600}.alarms
  #   Node.filter(:project_id => 123).subtree('/abc')
  #   Node.subtree('/test').recalculate
  #   # ...
  #
  # == Schemas
  # You can define your schema in the Model class itself:
  #
  #   class Comment < Sequel::Model(:comments)
  #     set_schema do
  #       primary_key :id
  #       foreign_key :post_id, :table => :posts, :on_delete => :cascade
  #
  #       varchar :name
  #       varchar :email
  #       text :comment
  #     end
  #
  #     # ...
  #
  #   end
  #
  # == Hooks
  # You can setup hooks here:
  # * before_save calls either
  # * before_create with
  # * after_create or if record already exists
  # * before_update with
  # * after_update and finally
  # * after_save
  # ... and here:
  # * before_destroy with
  # * after_destroy
  #
  # ...with:
  #
  #   class Example < Sequel::Model(:hooks)
  #     before_create { self.created_at = Time.now }
  #
  #     # ...
  #   end
  #
  # == Serialization of complexe attributes
  # Sometimes there are datatypes you can't natively map to your db. In this
  # case you can just do serialize:
  #
  #   class Serialized < Sequel::Model(:serialized)
  #     serialize :column1, :format => :yaml    # YAML is the default serialization method
  #     serialize :column2, :format => :marshal # serializes through marshalling
  #
  #     # ...
  #
  #   end
  class Model
    alias_method :model, :class
  end
end

require File.join(File.dirname(__FILE__), 'model/base')
require File.join(File.dirname(__FILE__), 'model/hooks')
require File.join(File.dirname(__FILE__), 'model/record')
require File.join(File.dirname(__FILE__), 'model/schema')
require File.join(File.dirname(__FILE__), 'model/relations')
require File.join(File.dirname(__FILE__), 'model/caching')

module Sequel
  class Model

    # Defines a method that returns a filtered dataset.
    def self.subset(name, *args, &block)
      dataset.meta_def(name) {filter(*args, &block)}
    end

    # Comprehensive description goes here!
    def primary_key_hash(value)
      # stock implementation
      {:id => value}
    end

    # Finds a single record according to the supplied filter, e.g.:
    #
    #   Ticket.find :author => 'Sharon' # => record
    #   Ticket.find {:price}17 # => Dataset
    #
    def self.find(*args, &block)
      dataset.filter(*args, &block).limit(1).first
      # dataset[cond.is_a?(Hash) ? cond : primary_key_hash(cond)]
    end
    
    def self.[](*args)
      args = args.first if (args.size == 1)
      dataset[(Hash === args) ? args : primary_key_hash(args)]
    end
    
    # Like find but invokes create with given conditions when record does not
    # exists.
    def self.find_or_create(cond)
      find(cond) || create(cond)
    end

    ############################################################################

    # Like delete_all, but invokes before_destroy and after_destroy hooks if used.
    def self.destroy_all
      has_hooks?(:before_destroy) || has_hooks?(:after_destroy) ? \
        dataset.destroy : dataset.delete
    end
    # Deletes all records.
    def self.delete_all
      dataset.delete
    end

    FIND_BY_REGEXP = /^find_by_(.*)/.freeze
    FILTER_BY_REGEXP = /^filter_by_(.*)/.freeze
    ALL_BY_REGEXP = /^all_by_(.*)/.freeze

    def self.method_missing(m, *args, &block) #:nodoc:
      Thread.exclusive do
        method_name = m.to_s
        if method_name =~ FIND_BY_REGEXP
          c = $1.to_sym
          meta_def(method_name) {|arg| find(c => arg)}
        elsif method_name =~ FILTER_BY_REGEXP
          c = $1.to_sym
          meta_def(method_name) {|arg| filter(c => arg)}
        elsif method_name =~ ALL_BY_REGEXP
          c = $1.to_sym
          meta_def(method_name) {|arg| filter(c => arg).all}
        elsif dataset.respond_to?(m)
          instance_eval("def #{m}(*args, &block); dataset.#{m}(*args, &block); end")
        end
      end
      respond_to?(m) ? send(m, *args, &block) : super(m, *args)
    end

    # Comprehensive description goes here!
    def self.join(*args)
      table_name = dataset.opts[:from].first
      dataset.join(*args).select(table_name.to_sym.ALL)
    end

    # Returns value of attribute.
    def [](field)
      @values[field]
    end
    # Sets value of attribute.
    def []=(field, value)
      @values[field] = value
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

    # Compares models by values.
    def ==(obj)
      (obj.class == model) && (obj.values == @values)
    end
    # Compares object by pkey.
    def ===(obj)
      (obj.class == model) && (obj.pkey == pkey)
    end

  end
end
