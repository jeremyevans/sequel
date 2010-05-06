# Adds the Sequel::Migration and Sequel::Migrator classes, which allow
# the user to easily group schema changes and migrate the database
# to a newer version or revert to a previous version.

module Sequel
  # The +Migration+ class describes a database migration that can be reversed.
  # Example:
  #
  #   migration1 = Sequel.migration do
  #     up do
  #       create_table :sessions do
  #         primary_key :id
  #         String :session_id, :size => 32, :unique => true
  #         DateTime :created_at
  #         text :data
  #       end
  #     end
  # 
  #     down do
  #       # You can use raw SQL if you need to
  #       self << 'DROP TABLE sessions'
  #     end
  #   end
  #
  #   migration2 = Sequel.migration do
  #     up do
  #       alter_table :items do
  #         add_column :category, String, :default => 'ruby'
  #       end
  #     end
  # 
  #     down do
  #       alter_table :items do
  #         drop_column :category
  #       end  
  #     end
  #   end
  #
  # To apply a migration to a database, you can invoke +apply+ with
  # the target database instance and the direction <tt>:up</tt> or <tt>:down</tt>, e.g.:
  #
  #   DB = Sequel.connect('sqlite://mydb')
  #   migration1.apply(DB, :up)
  #
  # See <tt>Sequel::Schema::Generator</tt> for the syntax to use for creating tables
  # with +create_table+, and <tt>Sequel::Schema::AlterTableGenerator</tt> for the
  # syntax to use when altering existing tables with +alter_table+.
  # Migrations act as a proxy for the database
  # given in +apply+, so inside the +down+ and +up+ blocks, you can act as though +self+
  # refers to the database, which allows you to use any of the <tt>Sequel::Database</tt>
  # instance methods directly.
  class Migration
    # Creates a new instance of the migration and sets the @db attribute.
    def initialize(db)
      @db = db
    end
    
    # Applies the migration to the supplied database in the specified
    # direction.
    def self.apply(db, direction)
      raise(ArgumentError, "Invalid migration direction specified (#{direction.inspect})") unless [:up, :down].include?(direction)
      new(db).send(direction)
    end

    # Returns the list of Migration descendants.
    def self.descendants
      @descendants ||= []
    end
    
    # Adds the new migration class to the list of Migration descendants.
    def self.inherited(base)
      descendants << base
    end
    
    # The default down action does nothing
    def down
    end
    
    # Intercepts method calls intended for the database and sends them along.
    def method_missing(method_sym, *args, &block)
      @db.send(method_sym, *args, &block)
    end

    # The default up action does nothing
    def up
    end
  end

  # Internal class used by the Sequel.migration DSL.
  class MigrationDSL < BasicObject
    # The underlying Migration class.
    attr_reader :migration

    def self.create(&block)
      new(&block).migration
    end

    # Create a new migration class, and instance_eval the block.
    def initialize(&block)
      @migration = Class.new(Sequel::Migration)
      instance_eval(&block)
    end

    # Defines the migration's down action.
    def down(&block)
      @migration.send(:define_method, :down, &block)
    end

    # Defines the migration's up action.
    def up(&block)
      @migration.send(:define_method, :up, &block)
    end
  end

  # A short cut for creating anonymous migration classes. For example,
  # this code:
  # 
  #   Sequel.migration do
  #     up do
  #       create_table(:artists) do
  #         primary_key :id
  #         String :name
  #       end
  #     end
  #     
  #     down do
  #       drop_table(:artists)
  #     end
  #   end
  #
  # is just a easier way of writing:
  # 
  #   Class.new(Sequel::Migration) do
  #     def up 
  #       create_table(:artists) do
  #         primary_key :id
  #         String :name
  #       end
  #     end
  #     
  #     def down 
  #       drop_table(:artists)
  #     end
  #   end
  def self.migration(&block)
    MigrationDSL.create(&block)
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
  # Migrations are generally run via the sequel command line tool,
  # using the -m and -M switches.  The -m switch specifies the migration
  # directory, and the -M switch specifies the version to which to migrate.
  # 
  # You can apply migrations using the Migrator API, as well (this is necessary
  # if you want to specify the version from which to migrate in addition to the version
  # to which to migrate).
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
  module Migrator
    DEFAULT_SCHEMA_COLUMN = :version
    DEFAULT_SCHEMA_TABLE = :schema_info
    MIGRATION_FILE_PATTERN = /\A\d+_.+\.rb\z/.freeze
    MIGRATION_SPLITTER = '_'.freeze

    # Exception class raised when there is an error with the migration
    # file structure.
    class Error < Sequel::Error
    end

    # Wrapper for run, maintaining backwards API compatibility
    def self.apply(db, directory, target = nil, current = nil)
      run(db, directory, :target => target, :current => current)
    end

    # Migrates the supplied database using the migration files in the the specified directory. Options:
    # * :column - The column in the :table argument storing the migration version (default: :version).
    # * :current - The current version of the database.  If not given, it is retrieved from the database
    #   using the :table and :column options.
    # * :table - The table containing the schema version (default: :schema_info).
    # * :target - The target version to which to migrate.  If not given, migrates to the maximum version.
    #
    # Examples: 
    #   Sequel::Migrator.run(DB, "migrations")
    #   Sequel::Migrator.run(DB, "migrations", :target=>15, :current=>10)
    #   Sequel::Migrator.run(DB, "app1/migrations", :column=> :app2_version)
    #   Sequel::Migrator.run(DB, "app2/migrations", :column => :app2_version, :table=>:schema_info2)
    def self.run(db, directory, opts={})
      raise(Error, "Must supply a valid migration path") unless directory and File.directory?(directory)
      raise(Error, "No current version available") unless current = opts[:current] || get_current_migration_version(db, opts)
      raise(Error, "No target version available") unless target  = opts[:target]  || latest_migration_version(directory)

      direction = current < target ? :up : :down
      
      classes = migration_classes(directory, target, current, direction)

      db.transaction do
        classes.each {|c| c.apply(db, direction)}
        set_current_migration_version(db, target, opts)
      end
      
      target
    end

    # Gets the current migration version stored in the database. If no version
    # number is stored, 0 is returned.
    def self.get_current_migration_version(db, opts={})
      (schema_info_dataset(db, opts).first || {})[opts[:column] || DEFAULT_SCHEMA_COLUMN] || 0
    end

    # Returns the latest version available in the specified directory.
    def self.latest_migration_version(directory)
      l = migration_files(directory).last
      l ? migration_version_from_file(File.basename(l)) : nil
    end

    # Returns a list of migration classes filtered for the migration range and
    # ordered according to the migration direction.
    def self.migration_classes(directory, target, current, direction)
      range = direction == :up ?
        (current + 1)..target : (target + 1)..current

      # Remove class definitions
      Migration.descendants.each do |c|
        Object.send(:remove_const, c.to_s) rescue nil
      end
      Migration.descendants.clear # remove any defined migration classes

      # load migration files
      migration_files(directory, range).each {|fn| load(fn)}
      
      # get migration classes
      classes = Migration.descendants
      classes.reverse! if direction == :down
      classes
    end
    
    # Returns any found migration files in the supplied directory.
    def self.migration_files(directory, range = nil)
      files = []
      Dir.new(directory).each do |file|
        next unless MIGRATION_FILE_PATTERN.match(file)
        version = migration_version_from_file(file)
        raise(Error, "Duplicate migration version: #{version}") if files[version]
        files[version] = File.join(directory, file)
      end
      1.upto(files.length - 1){|i| raise(Error, "Missing migration version: #{i}") unless files[i]}
      filtered = range ? files[range] : files
      filtered ? filtered.compact : []
    end
    
    # Returns the dataset for the schema_info table. If no such table
    # exists, it is automatically created.
    def self.schema_info_dataset(db, opts={})
      column = opts[:column] || DEFAULT_SCHEMA_COLUMN
      table  = opts[:table]  || DEFAULT_SCHEMA_TABLE
      db.create_table?(table){Integer column}
      db.alter_table(table){add_column column, Integer} unless db.from(table).columns.include?(column)
      db.from(table)
    end
    
    # Sets the current migration  version stored in the database.
    def self.set_current_migration_version(db, version, opts={})
      column = opts[:column] || DEFAULT_SCHEMA_COLUMN
      dataset = schema_info_dataset(db, opts)
      dataset.send(dataset.first ? :update : :insert, column => version)
    end

    # Return the integer migration version based on the filename.
    def self.migration_version_from_file(filename) # :nodoc:
      filename.split(MIGRATION_SPLITTER, 2).first.to_i
    end
    private_class_method :migration_version_from_file
  end
end
