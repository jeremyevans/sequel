module Sequel
  module Schema
    class Generator
      def initialize(db, table_name, &block)
        @db = db
        @table_name = table_name
        @columns = []
        @indexes = []
        @primary_key = nil
        instance_eval(&block)
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
      
      def create_info
        if @primary_key && !has_column?(@primary_key[:name])
          @columns.unshift(@primary_key)
        end
        [@table_name, @columns, @indexes]
      end
    end
  end
end

