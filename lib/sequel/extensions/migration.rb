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

  # The Migrator class performs migrations based on migration files in a 
  # specified directory. The migration files should be named using the
  # following pattern:
  # 
  #   <version>_<title>.rb
  #
  # For example, the following files are considered migration files:
  #   
  #   001_create_sessions.rb
  #   002_add_data_column.rb
  #   
  # You can also use timestamps as version numbers:
  #
  #   1273253850_create_sessions.rb
  #   1273257248_add_data_column.rb
  #
  # If any migration filenames use timestamps as version numbers, Sequel
  # uses the +TimestampMigrator+ to migrate, otherwise it uses the +IntegerMigrator+.
  # The +TimestampMigrator+ can handle migrations that are run out of order
  # as well as migrations with the same timestamp,
  # while the +IntegerMigrator+ is more strict and raises exceptions for missing
  # or duplicate migration files.
  #
  # The migration files should contain either one +Migration+
  # subclass or one <tt>Sequel.migration</tt> call.
  #
  # Migrations are generally run via the sequel command line tool,
  # using the -m and -M switches.  The -m switch specifies the migration
  # directory, and the -M switch specifies the version to which to migrate.
  # 
  # You can apply migrations using the Migrator API, as well (this is necessary
  # if you want to specify the version from which to migrate in addition to the version
  # to which to migrate).
  # To apply a migrator, the +apply+ method must be invoked with the database
  # instance, the directory of migration files and the target version. If
  # no current version is supplied, it is read from the database. The migrator
  # automatically creates a table (schema_info for integer migrations and
  # schema_migrations for timestamped migrations). in the database to keep track
  # of the current migration version. If no migration version is stored in the
  # database, the version is considered to be 0. If no target version is 
  # specified, the database is migrated to the latest version available in the
  # migration directory.
  #
  # For example, to migrate the database to the latest version:
  #
  #   Sequel::Migrator.apply(DB, '.')
  #
  # For example, to migrate the database all the way down:
  #
  #   Sequel::Migrator.apply(DB, '.', 0)
  #
  # For example, to migrate the database to version 4:
  #
  #   Sequel::Migrator.apply(DB, '.', 4)
  #
  # To migrate the database from version 1 to version 5:
  #
  #   Sequel::Migrator.apply(DB, '.', 5, 1)
  class Migrator
    MIGRATION_FILE_PATTERN = /\A\d+_.+\.rb\z/.freeze
    MIGRATION_SPLITTER = '_'.freeze
    MINIMUM_TIMESTAMP = 1104566400

    # Exception class raised when there is an error with the migrator's
    # file structure, database, or arguments.
    class Error < Sequel::Error
    end

    # Wrapper for +run+, maintaining backwards API compatibility
    def self.apply(db, directory, target = nil, current = nil)
      run(db, directory, :target => target, :current => current)
    end

    # Migrates the supplied database using the migration files in the the specified directory. Options:
    # * :column :: The column in the :table argument storing the migration version (default: :version).
    # * :current :: The current version of the database.  If not given, it is retrieved from the database
    #               using the :table and :column options.
    # * :table :: The table containing the schema version (default: :schema_info).
    # * :target :: The target version to which to migrate.  If not given, migrates to the maximum version.
    #
    # Examples: 
    #   Sequel::Migrator.run(DB, "migrations")
    #   Sequel::Migrator.run(DB, "migrations", :target=>15, :current=>10)
    #   Sequel::Migrator.run(DB, "app1/migrations", :column=> :app2_version)
    #   Sequel::Migrator.run(DB, "app2/migrations", :column => :app2_version, :table=>:schema_info2)
    def self.run(db, directory, opts={})
      migrator_class(directory).new(db, directory, opts).run
    end

    # Choose the Migrator subclass to use.  Uses the TimestampMigrator
    # if the version number appears to be a unix time integer for a year
    # after 2005, otherwise uses the IntegerMigrator.
    def self.migrator_class(directory)
      Dir.new(directory).each do |file|
        next unless MIGRATION_FILE_PATTERN.match(file)
        return TimestampMigrator if file.split(MIGRATION_SPLITTER, 2).first.to_i > MINIMUM_TIMESTAMP
      end
      IntegerMigrator
    end
    private_class_method :migrator_class
    
    # The column to use to hold the migration version number for integer migrations or
    # filename for timestamp migrations (defaults to :version for integer migrations and
    # :filename for timestamp migrations)
    attr_reader :column

    # The database related to this migrator
    attr_reader :db

    # The directory for this migrator's files
    attr_reader :directory

    # The dataset for this migrator, representing the +schema_info+ table for integer
    # migrations and the +schema_migrations+ table for timestamp migrations
    attr_reader :ds

    # All migration files in this migrator's directory
    attr_reader :files

    # The table to use to hold the applied migration data (defaults to :schema_info for
    # integer migrations and :schema_migrations for timestamp migrations)
    attr_reader :table

    # The target version for this migrator
    attr_reader :target

    # Setup the state for the migrator
    def initialize(db, directory, opts={})
      raise(Error, "Must supply a valid migration path") unless File.directory?(directory)
      @db = db
      @directory = directory
      @files = get_migration_files
      @table = opts[:table]  || self.class.const_get(:DEFAULT_SCHEMA_TABLE)
      @column = opts[:column] || self.class.const_get(:DEFAULT_SCHEMA_COLUMN)
      @ds = schema_dataset
    end

    private

    # Remove all migration classes.  Done by the migrator to ensure that
    # the correct migration classes are picked up.
    def remove_migration_classes
      # Remove class definitions
      Migration.descendants.each do |c|
        Object.send(:remove_const, c.to_s) rescue nil
      end
      Migration.descendants.clear # remove any defined migration classes
    end

    # Return the integer migration version based on the filename.
    def migration_version_from_file(filename)
      filename.split(MIGRATION_SPLITTER, 2).first.to_i
    end
  end

  # The default migrator, recommended in most cases.  Uses a simple incrementing
  # version number starting with 1, where missing or duplicate migration file
  # versions are not allowed.
  class IntegerMigrator < Migrator
    DEFAULT_SCHEMA_COLUMN = :version
    DEFAULT_SCHEMA_TABLE = :schema_info

    Error = Migrator::Error

    # The current version for this migrator
    attr_reader :current

    # The direction of the migrator, either :up or :down
    attr_reader :direction

    # The migrations used by this migrator
    attr_reader :migrations

    # Set up all state for the migrator instance
    def initialize(db, directory, opts={})
      super
      @target = opts[:target] || latest_migration_version
      @current = opts[:current] || current_migration_version
      @direction = current < target ? :up : :down
      @migrations = get_migrations

      raise(Error, "No current version available") unless current
      raise(Error, "No target version available") unless target
    end

    # Apply all migrations on the database
    def run
      migrations.zip(version_numbers).each do |m, v|
        db.transaction do
          m.apply(db, direction)
          set_migration_version(v)
        end
      end
      
      target
    end

    private

    # Gets the current migration version stored in the database. If no version
    # number is stored, 0 is returned.
    def current_migration_version
      ds.get(column) || 0
    end

    # Returns any found migration files in the supplied directory.
    def get_migration_files
      files = []
      Dir.new(directory).each do |file|
        next unless MIGRATION_FILE_PATTERN.match(file)
        version = migration_version_from_file(file)
        raise(Error, "Duplicate migration version: #{version}") if files[version]
        files[version] = File.join(directory, file)
      end
      1.upto(files.length - 1){|i| raise(Error, "Missing migration version: #{i}") unless files[i]}
      files
    end
    
    # Returns a list of migration classes filtered for the migration range and
    # ordered according to the migration direction.
    def get_migrations
      remove_migration_classes

      # load migration files
      files[up? ? (current + 1)..target : (target + 1)..current].compact.each{|f| load(f)}
      
      # get migration classes
      classes = Migration.descendants
      up? ? classes : classes.reverse
    end
    
    # Returns the latest version available in the specified directory.
    def latest_migration_version
      l = files.last
      l ? migration_version_from_file(File.basename(l)) : nil
    end
    
    # Returns the dataset for the schema_info table. If no such table
    # exists, it is automatically created.
    def schema_dataset
      c = column
      ds = db.from(table)
      if !db.table_exists?(table)
        db.create_table(table){Integer c, :default=>0, :null=>false}
      elsif !ds.columns.include?(c)
        db.alter_table(table){add_column c, Integer, :default=>0, :null=>false}
      end
      ds.insert(c=>0) if ds.empty?
      raise(Error, "More than 1 row in migrator table") if ds.count > 1
      ds
    end
    
    # Sets the current migration  version stored in the database.
    def set_migration_version(version)
      ds.update(column=>version)
    end

    # Whether or not this is an up migration
    def up?
      direction == :up
    end

    # An array of numbers corresponding to the migrations,
    # so that each number in the array is the migration version
    # that will be in affect after the migration is run.
    def version_numbers
      up? ? ((current+1)..target).to_a : (target..(current - 1)).to_a.reverse
    end
  end

  # The migrator used if any migration file version appears to be a timestamp.
  # Stores filenames of migration files, and can figure out which migrations
  # have not been applied and apply them, even if earlier migrations are added
  # after later migrations.  If you plan to do that, the responsibility is on
  # you to make sure the migrations don't conflict.
  class TimestampMigrator < Migrator
    DEFAULT_SCHEMA_COLUMN = :filename
    DEFAULT_SCHEMA_TABLE = :schema_migrations
    
    Error = Migrator::Error

    # Array of strings of applied migration filenames
    attr_reader :applied_migrations

    # Get tuples of migrations, filenames, and actions for each migration
    attr_reader :migration_tuples

    # Set up all state for the migrator instance
    def initialize(db, directory, opts={})
      super
      @target = opts[:target]
      @applied_migrations = get_applied_migrations
      @migration_tuples = get_migration_tuples
    end

    # Apply all migration tuples on the database
    def run
      migration_tuples.each do |m, f, direction|
        db.transaction do
          m.apply(db, direction)
          direction == :up ? ds.insert(column=>f) : ds.filter(column=>f).delete
        end
      end
      nil
    end

    private

    # Convert the schema_info table to the new schema_migrations table format,
    # using the version of the schema_info table and the current migration files.
    def convert_from_schema_info
      v = db[IntegerMigrator::DEFAULT_SCHEMA_TABLE].get(IntegerMigrator::DEFAULT_SCHEMA_COLUMN)
      ds = db.from(table)
      files.each do |path|
        f = File.basename(path)
        if migration_version_from_file(f) <= v
          ds.insert(column=>f)
        end
      end
    end

    # Returns filenames of all applied migrations
    def get_applied_migrations
      am = ds.select_order_map(column)
      missing_migration_files = am - files.map{|f| File.basename(f)}
      raise(Error, "Applied migration files not in file system: #{missing_migration_files.join(', ')}") if missing_migration_files.length > 0
      am
    end
    
    # Returns any migration files found in the migrator's directory.
    def get_migration_files
      files = []
      Dir.new(directory).each do |file|
        next unless MIGRATION_FILE_PATTERN.match(file)
        files << File.join(directory, file)
      end
      files.sort
    end
    
    # Returns tuples of migration, filename, and direction
    def get_migration_tuples
      remove_migration_classes
      up_mts = []
      down_mts = []
      ms = Migration.descendants
      files.each do |path|
        f = File.basename(path)
        if target
          if migration_version_from_file(f) > target
            if applied_migrations.include?(f)
              load(path)
              down_mts << [ms.last, f, :down]
            end
          elsif !applied_migrations.include?(f)
            load(path)
            up_mts << [ms.last, f, :up]
          end
        elsif !applied_migrations.include?(f)
          load(path)
          up_mts << [ms.last, f, :up]
        end
      end
      up_mts + down_mts.reverse
    end
    
    # Returns the dataset for the schema_migrations table. If no such table
    # exists, it is automatically created.
    def schema_dataset
      c = column
      ds = db.from(table)
      if !db.table_exists?(table)
        db.create_table(table){String c, :primary_key=>true}
        if db.table_exists?(:schema_info) and vha = db[:schema_info].all and vha.length == 1 and
           vha.first.keys == [:version] and vha.first.values.first.is_a?(Integer)
          convert_from_schema_info
        end
      elsif !ds.columns.include?(c)
        raise(Error, "Migrator table #{table} does not contain column #{c}")
      end
      ds
    end
  end
end
