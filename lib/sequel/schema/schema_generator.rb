module Sequel
  module Schema
    class Generator
      def initialize(db, table_name, &block)
        @db = db
        @table_name = table_name
        @columns = []
        @indexes = []
        instance_eval(&block)
      end
      
      def method_missing(type, name = nil, opts = nil)
        return super unless name
        column(name, type, opts)
      end
      
      def primary_key_name
        @primary_key ? @primary_key[:name] : nil
      end
      
      def primary_key(name, type = nil, opts = nil)
        @primary_key = @db.serial_primary_key_options.merge({
          :name => name
        })
        @primary_key.merge!({:type => type}) if type
        @primary_key.merge!(opts) if opts
        @primary_key
      end
      
      def column(name, type, opts = nil)
        @columns << {:name => name, :type => type}.merge(opts || {})
      end
      
      def foreign_key(name, opts = nil)
        @columns << {:name => name, :type => :integer}.merge(opts || {})
      end
      
      def has_column?(name)
        @columns.each {|c| return true if c[:name] == name}
        false
      end
      
      def index(columns, opts = nil)
        columns = [columns] unless columns.is_a?(Array)
        @indexes << {:columns => columns}.merge(opts || {})
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

