require 'rubygems'
require 'postgres'

module Sequel
  class Schema
    COMMA_SEPARATOR = ', '.freeze
    COLUMN_DEF = '%s %s'.freeze
    UNIQUE = ' UNIQUE'.freeze
    NOT_NULL = ' NOT NULL'.freeze
    DEFAULT = ' DEFAULT %s'.freeze
    PRIMARY_KEY = ' PRIMARY KEY'.freeze
    REFERENCES = ' REFERENCES %s'.freeze
    ON_DELETE = ' ON DELETE %s'.freeze
    AUTOINCREMENT = ' AUTOINCREMENT'.freeze
    
    RESTRICT = 'RESTRICT'.freeze
    CASCADE = 'CASCADE'.freeze
    NO_ACTION = 'NO ACTION'.freeze
    SET_NULL = 'SET NULL'.freeze
    SET_DEFAULT = 'SET DEFAULT'.freeze
    
    TYPES = Hash.new {|h, k| k}
    TYPES[:double] = 'double precision'
    
    def self.on_delete_action(action)
      case action
      when :restrict: RESTRICT
      when :cascade: CASCADE
      when :set_null: SET_NULL
      when :set_default: SET_DEFAULT
      else NO_ACTION
      end
    end
    
    def self.column_definition(column)
      c = COLUMN_DEF % [column[:name], TYPES[column[:type]]]
      c << UNIQUE if column[:unique]
      c << NOT_NULL if column[:null] == false
      c << DEFAULT % PGconn.quote(column[:default]) if column.include?(:default)
      c << PRIMARY_KEY if column[:primary_key]
      c << REFERENCES % column[:table] if column[:table]
      c << ON_DELETE % on_delete_action(column[:on_delete]) if 
        column[:on_delete]
      c << AUTOINCREMENT if column[:auto_increment]
      c
    end
  
    def self.create_table_column_list(columns)
      columns.map {|c| column_definition(c)}.join(COMMA_SEPARATOR)
    end
    
    CREATE_INDEX = 'CREATE INDEX %s ON %s (%s);'.freeze
    CREATE_UNIQUE_INDEX = 'CREATE UNIQUE INDEX %s ON %s (%s);'.freeze
    INDEX_NAME = '%s_%s_index'.freeze
    UNDERSCORE = '_'.freeze
    
    def self.index_definition(table_name, index)
      fields = index[:columns].join(COMMA_SEPARATOR)
      index_name = index[:name] || INDEX_NAME %
        [table_name, index[:columns].join(UNDERSCORE)]
      (index[:unique] ? CREATE_UNIQUE_INDEX : CREATE_INDEX) %
        [index_name, table_name, fields]
    end
    
    def self.create_indexes_sql(table_name, indexes)
      indexes.map {|i| index_definition(table_name, i)}.join
    end
  
    CREATE_TABLE = "CREATE TABLE %s (%s);".freeze
    
    def self.create_table_sql(name, columns, indexes = nil)
      sql = CREATE_TABLE % [name, create_table_column_list(columns)]
      sql << create_indexes_sql(name, indexes) if indexes && !indexes.empty?
      sql
    end
    
    DROP_TABLE = "DROP TABLE %s CASCADE;".freeze
    
    def self.drop_table_sql(name)
      DROP_TABLE % name
    end
    
    class Generator
      attr_reader :table_name
    
      def initialize(table_name, auto_primary_key = nil, &block)
        @table_name = table_name
        @primary_key = auto_primary_key
        @columns = []
        @indexes = []
        instance_eval(&block)
      end
      
      def primary_key(name, type = nil, opts = nil)
        @primary_key = {
          :name => name, 
          :type => type || :serial,
          :primary_key => true
        }.merge(opts || {})
      end
      
      def primary_key_name
        @primary_key && @primary_key[:name]
      end
      
      def column(name, type, opts = nil)
        @columns << {:name => name, :type => type}.merge(opts || {})
      end
      
      def foreign_key(name, opts)
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
      
      def create_sql
        if @primary_key && !has_column?(@primary_key[:name])
          @columns.unshift(@primary_key)
        end
        Schema.create_table_sql(@table_name, @columns, @indexes)
      end
      
      def drop_sql
        Schema.drop_table_sql(@table_name)
      end
    end
    
    attr_reader :instructions
    
    def initialize(&block)
      @instructions = []
      instance_eval(&block) if block
    end
    
    def auto_primary_key(name, type = nil, opts = nil)
      @auto_primary_key = {
        :name => name,
        :type => type || :serial,
        :primary_key => true
      }.merge(opts || {})
    end
    
    def create_table(table_name, &block)
      @instructions << Generator.new(table_name, @auto_primary_key, &block)
    end
    
    def create(db)
      @instructions.each do |s|
        db.execute(s.create_sql)
      end
    end
    
    def drop(db)
      @instructions.reverse_each do |s|
        db.execute(s.drop_sql) if db.table_exists?(s.table_name)
      end
    end
    
    def recreate(db)
      drop(db)
      create(db)
    end
  end
end

