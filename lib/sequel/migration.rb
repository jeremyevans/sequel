# The migration code is based on work by Florian Aßmann:
#   http://code.google.com/p/ruby-sequel/issues/detail?id=23

module Sequel
  # The Migration class describes a database migration that can be reversed.
  # The migration looks very similar to ActiveRecord (Rails) migrations, e.g.:
  #
  #   class CreateSessions < Sequel::Migration
  #     def up
  #       create_table :sessions do
  #         primary_key :id
  #         varchar   :session_id, :length => 32, :unique => true
  #         timestamp :created_at
  #         text      :data
  #       end
  #     end
  # 
  #     def down
  #       execute 'DROP TABLE sessions'
  #     end
  #   end
  #
  # To apply a migration to a database, you can invoke the #apply with
  # the target database instance and the direction :up or :down, e.g.:
  #
  #   DB = Sequel.open ('sqlite:///mydb')
  #   CreateSessions.apply(DB, :up)
  #
  class Migration
    # Creates a new instance of the migration and sets the @db attribute.
    def initialize(db)
      @db = db
    end
    
    # Adds the new migration class to the list of Migration descendants.
    def self.inherited(base)
      descendants << base
    end
    
    # Returns the list of Migration descendants.
    def self.descendants
      @descendants ||= []
    end
    
    def up; end #:nodoc:
    def down; end #:nodoc:
    
    # Applies the migration to the supplied database in the specified
    # direction.
    def self.apply(db, direction)
      obj = new(db)
      case direction
      when :up: obj.up
      when :down: obj.down
      else
        raise SequelError, "Invalid migration direction (#{direction})"
      end
    end

    # Intercepts method calls intended for the database and sends them along.
    def method_missing(method_sym, *args, &block)
      @db.send method_sym, *args, &block
    end
  end

  # The Migrator module performs migrations based on migration files in a 
  # specified directory. The migration files should be named using the
  # following pattern (in similar fashion to ActiveRecord migrations):
  # 
  #   <version>_<title>.rb
  #
  # For example, the following files are considered migration files:
  #   
  #   001_create_sessions.rb
  #   002_add_data_column.rb
  #   ...
  #
  # The migration files should contain one or more migration classes based
  # on Sequel::Migration.
  #
  # To apply a migration, the #apply method must be invoked with the database
  # instance, the directory of migration files and the target version. If
  # no current version is supplied, it is read from the database. The migrator
  # automatically creates a schema_info table in the database to keep track
  # of the current migration version. If no migration version is stored in the
  # database, the version is considered to be 0. If no target version is 
  # specified, the database is migrated to the latest version available in the
  # migration directory.
  #
  # For example, to migrate the database to the latest version:
  #
  #   Sequel::Migrator.apply(DB, '.')
  #
  # To migrate the database from version 1 to version 5:
  #
  #   Sequel::Migrator.apply(DB, '.', 5, 1)
  #
  module Migrator
    # Migrates the supplied database in the specified directory from the
    # current version to the target version. If no current version is
    # supplied, it is extracted from a schema_info table. The schema_info
    # table is automatically created and maintained by the apply function.
    def self.apply(db, directory, target = nil, current = nil)
      # determine current and target version and direction
      current ||= get_current_migration_version(db)
      target ||= latest_migration_version(directory)
      raise SequelError, "No current version available" if current.nil?
      raise SequelError, "No target version available" if target.nil?
      direction = current < target ? :up : :down
      
      classes = migration_classes(directory, target, current, direction)
      
      db.transaction do
        classes.each {|c| c.apply(db, direction)}
        set_current_migration_version(db, target)
      end
    end

    # Returns a list of migration classes filter for the migration range and
    # ordered according to the migration direction.
    def self.migration_classes(directory, target, current, direction)
      range = direction == :up ?
        (current + 1)..target : (target + 1)..current
        
      # load migration files
      Migration.descendants.clear # remove any defined migration classes
      migration_files(directory, range).each {|fn| load(fn)}
      
      # get migration classes
      classes = Migration.descendants
      classes.reverse! if direction == :down
      classes
    end
    
    MIGRATION_FILE_PATTERN = '[0-9][0-9][0-9]_*.rb'.freeze

    # Returns any found migration files in the supplied directory.
    def self.migration_files(directory, range = nil)
      pattern = File.join(directory, MIGRATION_FILE_PATTERN)
      files = Dir[pattern].inject([]) do |m, path|
        m[File.basename(path).to_i] = path
        m
      end
      filtered = range ? files[range] : files
      filtered ? filtered.compact : []
    end
    
    def self.latest_migration_version(directory)
      l = migration_files(directory).last
      l ? File.basename(l).to_i : nil
    end

    # Gets the current migration version stored in the database. If no version
    # number is stored, 0 is returned.
    def self.get_current_migration_version(db)
      r = schema_info_dataset(db).first
      r ? r[:version] : 0
    end
    
    # Sets the current migration  version stored in the database.
    def self.set_current_migration_version(db, version)
      dataset = schema_info_dataset(db)
      if dataset.first
        dataset.update(:version => version)
      else
        dataset << {:version => version}
      end
    end
    
    # Returns the dataset for the schema_info table. If no such table
    # exists, it is automatically created.
    def self.schema_info_dataset(db)
      unless db.table_exists?(:schema_info)
        db.create_table(:schema_info) {integer :version}
      end

      db[:schema_info]
    end
  end
end
