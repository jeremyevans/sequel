module Sequel
  class Migration
    def initialize(db)
      Deprecation.deprecate('Sequel::Migration', "require 'sequel/extensions/migration' first")
      require 'sequel/extensions/migration'
      @db = db
    end
    
    def self.apply(db, direction)
      Deprecation.deprecate('Sequel::Migration', "require 'sequel/extensions/migration' first")
      require 'sequel/extensions/migration'
      apply(db, direction)
    end

    def self.descendants
      Deprecation.deprecate('Sequel::Migration', "require 'sequel/extensions/migration' first")
      require 'sequel/extensions/migration'
      @descendants ||= []
    end
    
    def self.inherited(base)
      Deprecation.deprecate('Sequel::Migration', "require 'sequel/extensions/migration' first")
      require 'sequel/extensions/migration'
      descendants << base
    end
    
    def down
      Deprecation.deprecate('Sequel::Migration', "require 'sequel/extensions/migration' first")
      require 'sequel/extensions/migration'
    end
    
    def method_missing(method_sym, *args, &block)
      Deprecation.deprecate('Sequel::Migration', "require 'sequel/extensions/migration' first")
      require 'sequel/extensions/migration'
      @db.send(method_sym, *args, &block)
    end

    def up
      Deprecation.deprecate('Sequel::Migration', "require 'sequel/extensions/migration' first")
      require 'sequel/extensions/migration'
    end
  end

  module Migrator
    def self.apply(db, directory, target = nil, current = nil)
      Deprecation.deprecate('Sequel::Migrator', "require 'sequel/extensions/migration' first")
      require 'sequel/extensions/migration'
      apply(db, directory, target, current)
    end

    def self.get_current_migration_version(db)
      Deprecation.deprecate('Sequel::Migrator', "require 'sequel/extensions/migration' first")
      require 'sequel/extensions/migration'
      r = schema_info_dataset(db).first
      r ? r[:version] : 0
    end

    def self.latest_migration_version(directory)
      Deprecation.deprecate('Sequel::Migrator', "require 'sequel/extensions/migration' first")
      require 'sequel/extensions/migration'
      l = migration_files(directory).last
      l ? File.basename(l).to_i : nil
    end

    def self.migration_classes(directory, target, current, direction)
      Deprecation.deprecate('Sequel::Migrator', "require 'sequel/extensions/migration' first")
      require 'sequel/extensions/migration'
      migration_classes(directory, target, current, direction)
    end
    
    def self.migration_files(directory, range = nil)
      Deprecation.deprecate('Sequel::Migrator', "require 'sequel/extensions/migration' first")
      require 'sequel/extensions/migration'
      migration_files(directory, range)
    end
    
    def self.schema_info_dataset(db)
      Deprecation.deprecate('Sequel::Migrator', "require 'sequel/extensions/migration' first")
      require 'sequel/extensions/migration'
      db.create_table(:schema_info) {integer :version} unless db.table_exists?(:schema_info)
      db[:schema_info]
    end
    
    def self.set_current_migration_version(db, version)
      Deprecation.deprecate('Sequel::Migrator', "require 'sequel/extensions/migration' first")
      require 'sequel/extensions/migration'
      dataset = schema_info_dataset(db)
      dataset.send(dataset.first ? :update : :<<, :version => version)
    end
  end
end
