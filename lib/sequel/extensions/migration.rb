# Adds the Sequel::Migration and Sequel::Migrator classes, which allow
# the user to easily group schema changes and migrate the database
# to a newer version or revert to a previous version.
#
# To load the extension:
#
#   Sequel.extension :migration

module Sequel
  # Sequel's older migration class, available for backward compatibility.
  # Uses subclasses with up and down instance methods for each migration:
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
  #
  # Part of the +migration+ extension.
  class Migration
    # Set the database associated with this migration.
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

    # Don't allow transaction overriding in old migrations.
    def self.use_transactions
      nil
    end
    
    # The default down action does nothing
    def down
    end
    
    # Intercepts method calls intended for the database and sends them along.
    def method_missing(method_sym, *args, &block)
      @db.send(method_sym, *args, &block)
    end

    # This object responds to all methods the database responds to.
    def respond_to_missing?(meth, include_private)
      @db.respond_to?(meth, include_private)
    end

    # The default up action does nothing
    def up
    end
  end

  # Migration class used by the Sequel.migration DSL,
  # using instances for each migration, unlike the
  # +Migration+ class, which uses subclasses for each
  # migration. Part of the +migration+ extension.
  class SimpleMigration
    # Proc used for the down action
    attr_accessor :down

    # Proc used for the up action
    attr_accessor :up

    # Whether to use transactions for this migration, default depends on the
    # database.
    attr_accessor :use_transactions
    
    # Don't set transaction use by default.
    def initialize
      @use_transactions = nil
    end

    # Apply the appropriate block on the +Database+
    # instance using instance_eval.
    def apply(db, direction)
      raise(ArgumentError, "Invalid migration direction specified (#{direction.inspect})") unless [:up, :down].include?(direction)
      if prok = send(direction)
        db.instance_eval(&prok)
      end
    end
  end

  # Internal class used by the Sequel.migration DSL, part of the +migration+ extension.
  class MigrationDSL < BasicObject
    # The underlying Migration instance
    attr_reader :migration

    def self.create(&block)
      new(&block).migration
    end

    # Create a new migration class, and instance_eval the block.
    def initialize(&block)
      @migration = SimpleMigration.new
      Migration.descendants << migration
      instance_eval(&block)
    end

    # Defines the migration's down action.
    def down(&block)
      migration.down = block
    end

    # Disable the use of transactions for the related migration
    def no_transaction
      migration.use_transactions = false
    end

    # Enable the use of transactions for the related migration
    def transaction
      migration.use_transactions = true
    end

    # Defines the migration's up action.
    def up(&block)
      migration.up = block
    end

    # Creates a reversible migration.  This is the same as creating
    # the same block with +up+, but it also calls the block and attempts
    # to create a +down+ block that will reverse the changes made by
    # the block.
    #
    # There are no guarantees that this will work perfectly
    # in all cases, but it should work for most common cases.
    def change(&block)
      migration.up = block
      migration.down = MigrationReverser.new.reverse(&block)
    end
  end

  # Handles the reversing of reversible migrations.  Basically records
  # supported methods calls, translates them to reversed calls, and
  # returns them in reverse order.
  class MigrationReverser < Sequel::BasicObject
    def initialize
      @actions = []
    end

    # Reverse the actions for the given block.  Takes the block given
    # and returns a new block that reverses the actions taken by
    # the given block.
    def reverse(&block)
      begin
        instance_eval(&block)
      rescue
        just_raise = true
      end
      if just_raise
        Proc.new{raise Sequel::Error, 'irreversible migration method used, you may need to write your own down method'}
      else
        actions = @actions.reverse
        Proc.new do
          actions.each do |a|
            if a.last.is_a?(Proc)
              pr = a.pop
              send(*a, &pr)
            else
              send(*a)
            end
          end
        end
      end
    end

    private

    def add_column(*args)
      @actions << [:drop_column, args[0], args[1]]
    end

    def add_index(*args)
      @actions << [:drop_index, *args]
    end

    def alter_table(table, &block)
      @actions << [:alter_table, table, MigrationAlterTableReverser.new.reverse(&block)]
    end

    def create_join_table(*args)
      @actions << [:drop_join_table, *args]
    end

    def create_table(*args)
      @actions << [:drop_table, args.first]
    end

    def create_view(*args)
      @actions << [:drop_view, args.first]
    end

    def rename_column(table, name, new_name)
      @actions << [:rename_column, table, new_name, name]
    end

    def rename_table(table, new_name)
      @actions << [:rename_table, new_name, table]
    end
  end

  # Handles reversing an alter_table block in a reversible migration.
  class MigrationAlterTableReverser < Sequel::BasicObject
    def initialize
      @actions = []
    end

    def reverse(&block)
      instance_eval(&block)
      actions = @actions.reverse
      Proc.new{actions.each{|a| send(*a)}}
    end

    private

    def add_column(*args)
      @actions << [:drop_column, args.first]
    end

    def add_constraint(*args)
      @actions << [:drop_constraint, args.first]
    end

    def add_foreign_key(key, table, *args)
      @actions << [:drop_foreign_key, key, *args]
    end

    def add_primary_key(*args)
      raise if args.first.is_a?(Array)
      @actions << [:drop_column, args.first]
    end

    def add_index(*args)
      @actions << [:drop_index, *args]
    end
    alias add_full_text_index add_index
    alias add_spatial_index add_index

    def rename_column(name, new_name)
      @actions << [:rename_column, new_name, name]
    end
  end

  # The preferred method for writing Sequel migrations, using a DSL:
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
  # Designed to be used with the +Migrator+ class, part of the +migration+ extension.
  def self.migration(&block)
    MigrationDSL.create(&block)
  end

  # The +Migrator+ class performs migrations based on migration files in a 
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
  #
  # Part of the +migration+ extension.
  class Migrator
    MIGRATION_FILE_PATTERN = /\A(\d+)_.+\.rb\z/i.freeze
    MIGRATION_SPLITTER = '_'.freeze
    MINIMUM_TIMESTAMP = 20000101

    # Exception class raised when there is an error with the migrator's
    # file structure, database, or arguments.
    class Error < Sequel::Error
    end

    # Exception class raised when Migrator.check_current signals that it is
    # not current.
    class NotCurrentError < Error
    end

    # Wrapper for +run+, maintaining backwards API compatibility
    def self.apply(db, directory, target = nil, current = nil)
      run(db, directory, :target => target, :current => current)
    end

    # Raise a NotCurrentError unless the migrator is current, takes the same
    # arguments as #run.
    def self.check_current(*args)
      raise(NotCurrentError, 'migrator is not current') unless is_current?(*args)
    end

    # Return whether the migrator is current (i.e. it does not need to make
    # any changes).  Takes the same arguments as #run.
    def self.is_current?(db, directory, opts=OPTS)
      migrator_class(directory).new(db, directory, opts).is_current?
    end

    # Migrates the supplied database using the migration files in the the specified directory. Options:
    # :allow_missing_migration_files :: Don't raise an error if there are missing migration files.
    # :column :: The column in the :table argument storing the migration version (default: :version).
    # :current :: The current version of the database.  If not given, it is retrieved from the database
    #             using the :table and :column options.
    # :table :: The table containing the schema version (default: :schema_info).
    # :target :: The target version to which to migrate.  If not given, migrates to the maximum version.
    #
    # Examples: 
    #   Sequel::Migrator.run(DB, "migrations")
    #   Sequel::Migrator.run(DB, "migrations", :target=>15, :current=>10)
    #   Sequel::Migrator.run(DB, "app1/migrations", :column=> :app2_version)
    #   Sequel::Migrator.run(DB, "app2/migrations", :column => :app2_version, :table=>:schema_info2)
    def self.run(db, directory, opts=OPTS)
      migrator_class(directory).new(db, directory, opts).run
    end

    # Choose the Migrator subclass to use.  Uses the TimestampMigrator
    # if the version number appears to be a unix time integer for a year
    # after 2005, otherwise uses the IntegerMigrator.
    def self.migrator_class(directory)
      if self.equal?(Migrator)
        Dir.new(directory).each do |file|
          next unless MIGRATION_FILE_PATTERN.match(file)
          return TimestampMigrator if file.split(MIGRATION_SPLITTER, 2).first.to_i > MINIMUM_TIMESTAMP
        end
        IntegerMigrator
      else
        self
      end
    end
    
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
    def initialize(db, directory, opts=OPTS)
      raise(Error, "Must supply a valid migration path") unless File.directory?(directory)
      @db = db
      @directory = directory
      @allow_missing_migration_files = opts[:allow_missing_migration_files]
      @files = get_migration_files
      schema, table = @db.send(:schema_and_table, opts[:table]  || self.class.const_get(:DEFAULT_SCHEMA_TABLE))
      @table = schema ? Sequel::SQL::QualifiedIdentifier.new(schema, table) : table
      @column = opts[:column] || self.class.const_get(:DEFAULT_SCHEMA_COLUMN)
      @ds = schema_dataset
      @use_transactions = opts[:use_transactions]
    end

    private

    # If transactions should be used for the migration, yield to the block
    # inside a transaction.  Otherwise, just yield to the block.
    def checked_transaction(migration, &block)
      use_trans = if @use_transactions.nil?
        if migration.use_transactions.nil?
          @db.supports_transactional_ddl?
        else
          migration.use_transactions
        end
      else
        @use_transactions
      end

      if use_trans
        db.transaction(&block)
      else
        yield
      end
    end

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
  # versions are not allowed.  Part of the +migration+ extension.
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
    def initialize(db, directory, opts=OPTS)
      super
      @target = opts[:target] || latest_migration_version
      @current = opts[:current] || current_migration_version

      raise(Error, "No current version available") unless current
      raise(Error, "No target version available, probably because no migration files found or filenames don't follow the migration filename convention") unless target

      @direction = current < target ? :up : :down
      @migrations = get_migrations
    end

    # The integer migrator is current if the current version is the same as the target version.
    def is_current?
      current_migration_version == target
    end
    
    # Apply all migrations on the database
    def run
      migrations.zip(version_numbers).each do |m, v|
        t = Time.now
        lv = up? ? v : v + 1
        db.log_info("Begin applying migration version #{lv}, direction: #{direction}")
        checked_transaction(m) do
          m.apply(db, direction)
          set_migration_version(v)
        end
        db.log_info("Finished applying migration version #{lv}, direction: #{direction}, took #{sprintf('%0.6f', Time.now - t)} seconds")
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
        if version >= 20000101
          raise Migrator::Error, "Migration number too large, must use TimestampMigrator: #{file}"
        end
        raise(Error, "Duplicate migration version: #{version}") if files[version]
        files[version] = File.join(directory, file)
      end
      1.upto(files.length - 1){|i| raise(Error, "Missing migration version: #{i}") unless files[i]} unless @allow_missing_migration_files
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
      db.create_table?(table){Integer c, :default=>0, :null=>false}
      unless ds.columns.include?(c)
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
  # you to make sure the migrations don't conflict. Part of the +migration+ extension.
  class TimestampMigrator < Migrator
    DEFAULT_SCHEMA_COLUMN = :filename
    DEFAULT_SCHEMA_TABLE = :schema_migrations
    
    Error = Migrator::Error

    # Array of strings of applied migration filenames
    attr_reader :applied_migrations

    # Get tuples of migrations, filenames, and actions for each migration
    attr_reader :migration_tuples

    # Set up all state for the migrator instance
    def initialize(db, directory, opts=OPTS)
      super
      @target = opts[:target]
      @applied_migrations = get_applied_migrations
      @migration_tuples = get_migration_tuples
    end

    # The timestamp migrator is current if there are no migrations to apply
    # in either direction.
    def is_current?
      migration_tuples.empty?
    end
    
    # Apply all migration tuples on the database
    def run
      migration_tuples.each do |m, f, direction|
        t = Time.now
        db.log_info("Begin applying migration #{f}, direction: #{direction}")
        checked_transaction(m) do
          m.apply(db, direction)
          fi = f.downcase
          direction == :up ? ds.insert(column=>fi) : ds.filter(column=>fi).delete
        end
        db.log_info("Finished applying migration #{f}, direction: #{direction}, took #{sprintf('%0.6f', Time.now - t)} seconds")
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
      missing_migration_files = am - files.map{|f| File.basename(f).downcase}
      raise(Error, "Applied migration files not in file system: #{missing_migration_files.join(', ')}") if missing_migration_files.length > 0 && !@allow_missing_migration_files
      am
    end
    
    # Returns any migration files found in the migrator's directory.
    def get_migration_files
      files = []
      Dir.new(directory).each do |file|
        next unless MIGRATION_FILE_PATTERN.match(file)
        files << File.join(directory, file)
      end
      files.sort_by{|f| MIGRATION_FILE_PATTERN.match(File.basename(f))[1].to_i}
    end
    
    # Returns tuples of migration, filename, and direction
    def get_migration_tuples
      remove_migration_classes
      up_mts = []
      down_mts = []
      ms = Migration.descendants
      files.each do |path|
        f = File.basename(path)
        fi = f.downcase
        if target
          if migration_version_from_file(f) > target
            if applied_migrations.include?(fi)
              load(path)
              down_mts << [ms.last, f, :down]
            end
          elsif !applied_migrations.include?(fi)
            load(path)
            up_mts << [ms.last, f, :up]
          end
        elsif !applied_migrations.include?(fi)
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
