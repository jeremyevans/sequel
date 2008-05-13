module Sequel
  module Schema
    class Generator
      def initialize(db, &block)
        @db = db
        @columns = []
        @indexes = []
        @primary_key = nil
        instance_eval(&block) if block
      end
      
      def check(*args, &block)
        @columns << {:name => nil, :type => :check, :check => block || args}
      end

      def column(name, type, opts = {})
        @columns << {:name => name, :type => type}.merge(opts)
        index(name) if opts[:index]
      end
      
      def constraint(name, *args, &block)
        @columns << {:name => name, :type => :check, :check => block || args}
      end
      
      def create_info
        @columns.unshift(@primary_key) if @primary_key && !has_column?(primary_key_name)
        [@columns, @indexes]
      end

      def foreign_key(name, opts = {})
        @columns << {:name => name, :type => :integer}.merge(opts)
        index(name) if opts[:index]
      end
      
      def full_text_index(columns, opts = {})
        index(columns, opts.merge(:type => :full_text))
      end
      
      def has_column?(name)
        @columns.any?{|c| c[:name] == name}
      end
      
      def index(columns, opts = {})
        @indexes << {:columns => Array(columns)}.merge(opts)
      end
      
      def method_missing(type, name = nil, opts = {})
        name ? column(name, type, opts) : super
      end
      
      def primary_key(name, *args)
        @primary_key = @db.serial_primary_key_options.merge({:name => name})
        
        if opts = args.pop
          opts = {:type => opts} unless opts.is_a?(Hash)
          if type = args.pop
            opts.merge!(:type => type)
          end
          @primary_key.merge!(opts)
        end
        @primary_key
      end
      
      def primary_key_name
        @primary_key[:name] if @primary_key
      end
      
      def spatial_index(columns, opts = {})
        index(columns, opts.merge(:type => :spatial))
      end

      def unique(columns, opts = {})
        index(columns, opts.merge(:unique => true))
      end
    end
  
    class AlterTableGenerator
      attr_reader :operations
      
      def initialize(db, &block)
        @db = db
        @operations = []
        instance_eval(&block) if block
      end
      
      def add_column(name, type, opts = {})
        @operations << {:op => :add_column, :name => name, :type => type}.merge(opts)
      end
      
      def add_constraint(name, *args, &block)
        @operations << {:op => :add_constraint, :name => name, :type => :check, \
          :check => block || args}
      end

      def add_full_text_index(columns, opts = {})
        add_index(columns, {:type=>:full_text}.merge(opts))
      end
      
      def add_index(columns, opts = {})
        @operations << {:op => :add_index, :columns => Array(columns)}.merge(opts)
      end
      
      def add_spatial_index(columns, opts = {})
        add_index(columns, {:type=>:spatial}.merge(opts))
      end
      
      def drop_column(name)
        @operations << {:op => :drop_column, :name => name}
      end
      
      def drop_constraint(name)
        @operations << {:op => :drop_constraint, :name => name}
      end
      
      def drop_index(columns)
        @operations << {:op => :drop_index, :columns => Array(columns)}
      end

      def rename_column(name, new_name, opts = {})
        @operations << {:op => :rename_column, :name => name, :new_name => new_name}.merge(opts)
      end
      
      def set_column_default(name, default)
        @operations << {:op => :set_column_default, :name => name, :default => default}
      end

      def set_column_type(name, type)
        @operations << {:op => :set_column_type, :name => name, :type => type}
      end
    end
  end
end

