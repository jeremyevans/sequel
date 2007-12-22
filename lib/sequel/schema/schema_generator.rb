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
      
      def method_missing(type, name = nil, opts = {})
        name ? column(name, type, opts) : super
      end
      
      def primary_key_name
        @primary_key ? @primary_key[:name] : nil
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
      
      def column(name, type, opts = {})
        @columns << {:name => name, :type => type}.merge(opts)
        index(name) if opts[:index]
      end
      
      def foreign_key(name, opts = {})
        @columns << {:name => name, :type => :integer}.merge(opts)
        index(name) if opts[:index]
      end
      
      def has_column?(name)
        @columns.each {|c| return true if c[:name] == name}
        false
      end
      
      def index(columns, opts = {})
        columns = [columns] unless columns.is_a?(Array)
        @indexes << {:columns => columns}.merge(opts)
      end
      
      def unique(columns, opts = {})
        index(columns, {:unique => true}.merge(opts))
      end
      
      def create_info
        if @primary_key && !has_column?(@primary_key[:name])
          @columns.unshift(@primary_key)
        end
        [@columns, @indexes]
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
        @operations << {
          :op => :add_column,
          :name => name,
          :type => type
        }.merge(opts)
      end
      
      def drop_column(name)
        @operations << {
          :op => :drop_column,
          :name => name
        }
      end
      
      def rename_column(name, new_name, opts = {})
        @operations << {
          :op => :rename_column,
          :name => name,
          :new_name => new_name
        }.merge(opts)
      end
      
      def set_column_type(name, type)
        @operations << {
          :op => :set_column_type,
          :name => name,
          :type => type
        }
      end
      
      def set_column_default(name, default)
        @operations << {
          :op => :set_column_default,
          :name => name,
          :default => default
        }
      end
      
      def add_index(columns, opts = {})
        columns = [columns] unless columns.is_a?(Array)
        @operations << {
          :op => :add_index,
          :columns => columns
        }.merge(opts)
      end
      
      def drop_index(columns)
        columns = [columns] unless columns.is_a?(Array)
        @operations << {
          :op => :drop_index,
          :columns => columns
        }
      end
    end
  end
end

