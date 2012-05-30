module Sequel
  # The Schema module holds the schema generators.
  module Schema
    # Schema::Generator is an internal class that the user is not expected
    # to instantiate directly.  Instances are created by Database#create_table.
    # It is used to specify table creation parameters.  It takes a Database
    # object and a block of column/index/constraint specifications, and
    # gives the Database a table description, which the database uses to
    # create a table.
    #
    # Schema::Generator has some methods but also includes method_missing,
    # allowing users to specify column type as a method instead of using
    # the column method, which makes for a nicer DSL.
    #
    # For more information on Sequel's support for schema modification, see
    # the {"Migrations and Schema Modification" guide}[link:files/doc/migration_rdoc.html].
    class Generator
      # Classes specifying generic types that Sequel will convert to database-specific types.
      GENERIC_TYPES=[String, Integer, Fixnum, Bignum, Float, Numeric, BigDecimal,
      Date, DateTime, Time, File, TrueClass, FalseClass]
      
      # Return the column hashes created by this generator
      attr_reader :columns

      # Return the constraint hashes created by this generator
      attr_reader :constraints

      # Return the index hashes created by this generator
      attr_reader :indexes

      # Set the database in which to create the table, and evaluate the block
      # in the context of this object.
      def initialize(db, &block)
        @db = db
        @columns = []
        @indexes = []
        @constraints = []
        @primary_key = nil
        instance_eval(&block) if block
        @columns.unshift(@primary_key) if @primary_key && !has_column?(primary_key_name)
      end
      
      # Add a method for each of the given types that creates a column
      # with that type as a constant.  Types given should either already
      # be constants/classes or a capitalized string/symbol with the same name
      # as a constant/class.
      def self.add_type_method(*types)
        types.each do |type|
          class_eval("def #{type}(name, opts={}); column(name, #{type}, opts); end", __FILE__, __LINE__)
        end
      end
      
      # Add an unnamed constraint to the DDL, specified by the given block
      # or args:
      #
      #   check(:num=>1..5) # CHECK num >= 1 AND num <= 5
      #   check{num > 5} # CHECK num > 5
      def check(*args, &block)
        constraint(nil, *args, &block)
      end

      # Add a column with the given name, type, and opts to the DDL. 
      #
      #   column :num, :integer
      #   # num INTEGER
      #
      #   column :name, String, :null=>false, :default=>'a'
      #   # name varchar(255) NOT NULL DEFAULT 'a'
      #
      #   inet :ip
      #   # ip inet
      #
      # You can also create columns via method missing, so the following are
      # equivalent:
      #
      #   column :number, :integer
      #   integer :number
      #
      # The following options are supported:
      #
      # :default :: The default value for the column.
      # :deferrable :: This ensure Referential Integrity will work even if
      #                reference table will use for its foreign key a value that does not
      #                exists(yet) on referenced table. Basically it adds
      #                DEFERRABLE INITIALLY DEFERRED on key creation.
      # :index :: Create an index on this column.  If given a hash, use the hash as the
      #           options for the index.
      # :key :: For foreign key columns, the column in the associated table
      #         that this column references.  Unnecessary if this column
      #         references the primary key of the associated table, except if you are
      #         using MySQL.
      # :null :: Mark the column as allowing NULL values (if true),
      #          or not allowing NULL values (if false).  If unspecified, will default
      #          to whatever the database default is.
      # :on_delete :: Specify the behavior of this column when being deleted
      #               (:restrict, :cascade, :set_null, :set_default, :no_action).
      # :on_update :: Specify the behavior of this column when being updated
      #               (:restrict, :cascade, :set_null, :set_default, :no_action).
      # :primary_key :: Make the column as a single primary key column.  This should only
      #                 be used if you have a single, nonautoincrementing primary key column.
      # :unique :: Mark the column as unique, generally has the same effect as
      #            creating a unique index on the column.
      def column(name, type, opts = {})
        columns << {:name => name, :type => type}.merge(opts)
        if index_opts = opts[:index]
          index(name, index_opts.is_a?(Hash) ? index_opts : {})
        end
      end
      
      # Adds a named constraint (or unnamed if name is nil) to the DDL,
      # with the given block or args.
      #
      #   constraint(:blah, :num=>1..5) # CONSTRAINT blah CHECK num >= 1 AND num <= 5
      #   check(:foo){num > 5} # CONSTRAINT foo CHECK num > 5
      def constraint(name, *args, &block)
        constraints << {:name => name, :type => :check, :check => block || args}
      end
      
      # Add a foreign key in the table that references another table to the DDL. See column
      # for available options.
      #
      #   foreign_key(:artist_id) # artist_id INTEGER
      #   foreign_key(:artist_id, :artists) # artist_id INTEGER REFERENCES artists
      #   foreign_key(:artist_id, :artists, :key=>:id) # artist_id INTEGER REFERENCES artists(id)
      #
      # If you want a foreign key constraint without adding a column (usually because it is a
      # composite foreign key), you can provide an array of columns as the first argument, and
      # you can provide the :name option to name the constraint:
      #
      #   foreign_key([:artist_name, :artist_location], :artists, :name=>:artist_fk)
      #   # ADD CONSTRAINT artist_fk FOREIGN KEY (artist_name, artist_location) REFERENCES artists
      def foreign_key(name, table=nil, opts = {})
        opts = case table
        when Hash
          table.merge(opts)
        when Symbol
          opts.merge(:table=>table)
        when NilClass
          opts
        else
          raise(Error, "The second argument to foreign_key should be a Hash, Symbol, or nil")
        end
        return composite_foreign_key(name, opts) if name.is_a?(Array)
        column(name, Integer, opts)
      end

      # Add a full text index on the given columns to the DDL.
      #
      # PostgreSQL specific options:
      # :language :: Set a language to use for the index (default: simple).
      #
      # Microsoft SQL Server specific options:
      # :key_index :: The KEY INDEX to use for the full text index.
      def full_text_index(columns, opts = {})
        index(columns, opts.merge(:type => :full_text))
      end
      
      # True if the DDL includes the creation of a column with the given name.
      def has_column?(name)
        columns.any?{|c| c[:name] == name}
      end
      
      # Add an index on the given column(s) with the given options to the DDL.
      # General options:
      #
      # :name :: The name to use for the index. If not given, a default name
      #          based on the table and columns is used.
      # :type :: The type of index to use (only supported by some databases)
      # :unique :: Make the index unique, so duplicate values are not allowed.
      # :where :: Create a partial index (only supported by some databases)
      #
      # PostgreSQL specific options:
      #
      # :concurrently :: Create the index concurrently, so it doesn't block
      #                  operations on the table while the index is being
      #                  built.
      # :op_class :: Use a specific operator class in the index.
      #
      # Microsoft SQL Server specific options:
      #
      # :include :: Include additional column values in the index, without
      #             actually indexing on those values.
      #
      #   index :name
      #   # CREATE INDEX table_name_index ON table (name)
      #
      #   index [:artist_id, :name]
      #   # CREATE INDEX table_artist_id_name_index ON table (artist_id, name)
      def index(columns, opts = {})
        indexes << {:columns => Array(columns)}.merge(opts)
      end
      
      # Add a column with the given type, name, and opts to the DDL.  See +column+ for available
      # options.
      def method_missing(type, name = nil, opts = {})
        name ? column(name, type, opts) : super
      end
      
      # Adds an autoincrementing primary key column or a primary key constraint to the DDL.
      # To create a constraint, the first argument should be an array of column symbols
      # specifying the primary key columns. To create an autoincrementing primary key
      # column, a single symbol can be used. In both cases, an options hash can be used
      # as the second argument.
      # 
      # If you want to create a primary key column that is not autoincrementing, you
      # should not use this method.  Instead, you should use the regular +column+ method
      # with a <tt>:primary_key=>true</tt> option.
      # 
      # Examples:
      #   primary_key(:id)
      #   primary_key([:street_number, :house_number])
      def primary_key(name, *args)
        return composite_primary_key(name, *args) if name.is_a?(Array)
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

      # The name of the primary key for this generator, if it has a primary key.
      def primary_key_name
        @primary_key[:name] if @primary_key
      end
      
      # Add a spatial index on the given columns to the DDL.
      def spatial_index(columns, opts = {})
        index(columns, opts.merge(:type => :spatial))
      end

      # Add a unique constraint on the given columns to the DDL.
      #
      #   unique(:name) # UNIQUE (name)
      def unique(columns, opts = {})
        constraints << {:type => :unique, :columns => Array(columns)}.merge(opts)
      end

      private

      # Add a composite primary key constraint
      def composite_primary_key(columns, *args)
        opts = args.pop || {}
        constraints << {:type => :primary_key, :columns => columns}.merge(opts)
      end

      # Add a composite foreign key constraint
      def composite_foreign_key(columns, opts)
        constraints << {:type => :foreign_key, :columns => columns}.merge(opts)
      end
      
      add_type_method(*GENERIC_TYPES)
    end
  
    # Schema::AlterTableGenerator is an internal class that the user is not expected
    # to instantiate directly.  Instances are created by Database#alter_table.
    # It is used to specify table alteration parameters.  It takes a Database
    # object and a block of operations to perform on the table, and
    # gives the Database an array of table altering operations, which the database uses to
    # alter a table's description.
    #
    # For more information on Sequel's support for schema modification, see
    # the {"Migrations and Schema Modification" guide}[link:files/doc/migration_rdoc.html].
    class AlterTableGenerator
      # An array of DDL operations to perform
      attr_reader :operations
      
      # Set the Database object to which to apply the DDL, and evaluate the
      # block in the context of this object.
      def initialize(db, &block)
        @db = db
        @operations = []
        instance_eval(&block) if block
      end
      
      # Add a column with the given name, type, and opts to the DDL for the table.
      # See Generator#column for the available options.
      #
      #   add_column(:name, String) # ADD COLUMN name varchar(255)
      def add_column(name, type, opts = {})
        @operations << {:op => :add_column, :name => name, :type => type}.merge(opts)
      end
      
      # Add a constraint with the given name and args to the DDL for the table.
      # See Generator#constraint.
      #
      #   add_constraint(:valid_name, :name.like('A%'))
      #   # ADD CONSTRAINT valid_name CHECK (name LIKE 'A%')
      def add_constraint(name, *args, &block)
        @operations << {:op => :add_constraint, :name => name, :type => :check, :check => block || args}
      end

      # Add a unique constraint to the given column(s)
      #
      #   add_unique_constraint(:name) # ADD UNIQUE (name)
      #   add_unique_constraint(:name, :name=>:unique_name) # ADD CONSTRAINT unique_name UNIQUE (name)
      def add_unique_constraint(columns, opts = {})
        @operations << {:op => :add_constraint, :type => :unique, :columns => Array(columns)}.merge(opts)
      end

      # Add a foreign key with the given name and referencing the given table
      # to the DDL for the table.  See Generator#column for the available options.
      #
      # You can also pass an array of column names for creating composite foreign
      # keys. In this case, it will assume the columns exist and will only add
      # the constraint.  You can provide a :name option to name the constraint.
      #
      # NOTE: If you need to add a foreign key constraint to a single existing column
      # use the composite key syntax even if it is only one column.
      #
      #   add_foreign_key(:artist_id, :table) # ADD COLUMN artist_id integer REFERENCES table
      #   add_foreign_key([:name], :table) # ADD FOREIGN KEY (name) REFERENCES table
      def add_foreign_key(name, table, opts = {})
        return add_composite_foreign_key(name, table, opts) if name.is_a?(Array)
        add_column(name, Integer, {:table=>table}.merge(opts))
      end
      
      # Add a full text index on the given columns to the DDL for the table.
      # See Generator#index for available options.
      def add_full_text_index(columns, opts = {})
        add_index(columns, {:type=>:full_text}.merge(opts))
      end
      
      # Add an index on the given columns to the DDL for the table.  See
      # Generator#index for available options.
      #
      #   add_index(:artist_id) # CREATE INDEX table_artist_id_index ON table (artist_id)
      def add_index(columns, opts = {})
        @operations << {:op => :add_index, :columns => Array(columns)}.merge(opts)
      end
      
      # Add a primary key to the DDL for the table.  See Generator#column
      # for the available options.  Like +add_foreign_key+, if you specify
      # the column name as an array, it just creates a constraint:
      #
      #   add_primary_key(:id) # ADD COLUMN id serial PRIMARY KEY
      #   add_primary_key([:artist_id, :name]) # ADD PRIMARY KEY (artist_id, name)
      def add_primary_key(name, opts = {})
        return add_composite_primary_key(name, opts) if name.is_a?(Array)
        opts = @db.serial_primary_key_options.merge(opts)
        add_column(name, opts.delete(:type), opts)
      end
      
      # Add a spatial index on the given columns to the DDL for the table.
      # See Generator#index for available options.
      def add_spatial_index(columns, opts = {})
        add_index(columns, {:type=>:spatial}.merge(opts))
      end
      
      # Remove a column from the DDL for the table.
      #
      #   drop_column(:artist_id) # DROP COLUMN artist_id
      #   drop_column(:artist_id, :cascade=>true) # DROP COLUMN artist_id CASCADE
      def drop_column(name, opts={})
        @operations << {:op => :drop_column, :name => name}.merge(opts)
      end
      
      # Remove a constraint from the DDL for the table. MySQL/SQLite specific options:
      #
      # :type :: Set the type of constraint to drop, either :primary_key, :foreign_key,
      #          or :unique.
      #
      #   drop_constraint(:unique_name) # DROP CONSTRAINT unique_name
      #   drop_constraint(:unique_name, :cascade=>true) # DROP CONSTRAINT unique_name CASCADE
      def drop_constraint(name, opts={})
        @operations << {:op => :drop_constraint, :name => name}.merge(opts)
      end
      
      # Remove an index from the DDL for the table. General options:
      #
      # :name :: The name of the index to drop.  If not given, uses the same name
      #          that would be used by add_index with the same columns.
      #
      # PostgreSQL specific options:
      #
      # :cascade :: Cascade the index drop to dependent objects.
      # :concurrently :: Drop the index using CONCURRENTLY, which doesn't block
      #                  operations on the table.  Supported in PostgreSQL 9.2+.
      # :if_exists :: Only drop the index if it already exists.
      #
      #   drop_index(:artist_id) # DROP INDEX table_artist_id_index
      #   drop_index([:a, :b]) # DROP INDEX table_a_b_index
      #   drop_index([:a, :b], :name=>:foo) # DROP INDEX foo
      def drop_index(columns, options={})
        @operations << {:op => :drop_index, :columns => Array(columns)}.merge(options)
      end

      # Modify a column's name in the DDL for the table.
      #
      #   rename_column(:name, :artist_name) # RENAME COLUMN name TO artist_name
      def rename_column(name, new_name, opts = {})
        @operations << {:op => :rename_column, :name => name, :new_name => new_name}.merge(opts)
      end
      
      # Modify a column's default value in the DDL for the table.
      #
      #   set_column_default(:artist_name, 'a') # ALTER COLUMN artist_name SET DEFAULT 'a'
      def set_column_default(name, default)
        @operations << {:op => :set_column_default, :name => name, :default => default}
      end

      # Modify a column's type in the DDL for the table.
      #
      #   set_column_type(:artist_name, 'char(10)') # ALTER COLUMN artist_name TYPE char(10)
      #
      # PostgreSQL specific options:
      #
      # :using :: Add a USING clause that specifies how to convert existing values to new values.
      def set_column_type(name, type, opts={})
        @operations << {:op => :set_column_type, :name => name, :type => type}.merge(opts)
      end
      
      # Modify a column's NOT NULL constraint.
      #
      #   set_column_allow_null(:artist_name, false) # ALTER COLUMN artist_name SET NOT NULL
      def set_column_allow_null(name, allow_null)
        @operations << {:op => :set_column_null, :name => name, :null => allow_null}
      end

      private

      # Add a composite primary key constraint
      def add_composite_primary_key(columns, opts)
        @operations << {:op => :add_constraint, :type => :primary_key, :columns => columns}.merge(opts)
      end

      # Add a composite foreign key constraint
      def add_composite_foreign_key(columns, table, opts)
        @operations << {:op => :add_constraint, :type => :foreign_key, :columns => columns, :table => table}.merge(opts)
      end
    end
  end
end

