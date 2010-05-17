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
      
      # Return the columns created by this generator
      attr_reader :columns

      # Return the constraints created by this generator
      attr_reader :constraints

      # Return the indexes created by this generator
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
      
      # Add a unnamed constraint to the DDL, specified by the given block
      # or args.
      def check(*args, &block)
        constraint(nil, *args, &block)
      end

      # Add a column with the given name, type, and opts to the DDL. 
      #
      # You can also create columns via method missing, so the following are
      # equivalent:
      #
      #   column :number, :integer
      #   integer :number
      #
      # The following options are supported:
      #
      # * :default - The default value for the column.
      # * :index - Create an index on this column.
      # * :key - For foreign key columns, the column in the associated table
      #   that this column references.  Unnecessary if this column
      #   references the primary key of the associated table.
      # * :null - Mark the column as allowing NULL values (if true),
      #   or not allowing NULL values (if false).  If unspecified, will default
      #   to whatever the database default is.
      # * :on_delete - Specify the behavior of this column when being deleted.
      #   See Schema::SQL#on_delete_clause for options.
      # * :on_update - Specify the behavior of this column when being updated.
      #   See Schema::SQL#on_delete_clause for options.
      # * :size - The size of the column, generally used with string
      #   columns to specify the maximum number of characters the column will hold.
      #   An array of two integers can be provided to set the size and the
      #   precision, respectively, of decimal columns.
      # * :unique - Mark the column as unique, generally has the same effect as
      #   creating a unique index on the column.
      # * :unsigned - Make the column type unsigned, only useful for integer
      #   columns.
      def column(name, type, opts = {})
        columns << {:name => name, :type => type}.merge(opts)
        index(name) if opts[:index]
      end
      
      # Adds a named constraint (or unnamed if name is nil) to the DDL,
      # with the given block or args.
      def constraint(name, *args, &block)
        constraints << {:name => name, :type => :check, :check => block || args}
      end
      
      # Add a foreign key in the table that references another table to the DDL. See column
      # for available options.
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
      def full_text_index(columns, opts = {})
        index(columns, opts.merge(:type => :full_text))
      end
      
      # True if the DDL includes the creation of a column with the given name.
      def has_column?(name)
        columns.any?{|c| c[:name] == name}
      end
      
      # Add an index on the given column(s) with the given options to the DDL.
      # The available options are:
      #
      # * :type - The type of index to use (only supported by some databases)
      # * :unique - Make the index unique, so duplicate values are not allowed.
      # * :where - Create a partial index (only supported by some databases)
      def index(columns, opts = {})
        indexes << {:columns => Array(columns)}.merge(opts)
      end
      
      # Add a column with the given type, name, and opts to the DDL.  See column for available
      # options.
      def method_missing(type, name = nil, opts = {})
        name ? column(name, type, opts) : super
      end
      
      # Add primary key information to the DDL. Takes between one and three
      # arguments. The last one is an options hash as for Generator#column.
      # The first one distinguishes two modes: an array of existing column
      # names adds a composite primary key constraint. A single symbol adds a
      # new column of that name and makes it the primary key. In that case the
      # optional middle argument denotes the type.
      # 
      # Examples:
      #   primary_key(:id)
      #   primary_key(:zip_code, :null => false)
      #   primary_key([:street_number, :house_number])
      #   primary_key(:id, :string, :auto_increment => false)
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

      # The name of the primary key for this table, if it has a primary key.
      def primary_key_name
        @primary_key[:name] if @primary_key
      end
      
      # Add a spatial index on the given columns to the DDL.
      def spatial_index(columns, opts = {})
        index(columns, opts.merge(:type => :spatial))
      end

      # Add a unique constraint on the given columns to the DDL.
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
    # gives the Database a table an array of operations, which the database uses to
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
      def add_column(name, type, opts = {})
        @operations << {:op => :add_column, :name => name, :type => type}.merge(opts)
      end
      
      # Add a constraint with the given name and args to the DDL for the table.
      # See Generator#constraint.
      def add_constraint(name, *args, &block)
        @operations << {:op => :add_constraint, :name => name, :type => :check, :check => block || args}
      end

      # Add a unique constraint to the given column(s)
      def add_unique_constraint(columns, opts = {})
        @operations << {:op => :add_constraint, :type => :unique, :columns => Array(columns)}.merge(opts)
      end

      # Add a foreign key with the given name and referencing the given table
      # to the DDL for the table.  See Generator#column for the available options.
      #
      # You can also pass an array of column names for creating composite foreign
      # keys. In this case, it will assume the columns exists and will only add
      # the constraint.
      #
      # NOTE: If you need to add a foreign key constraint to an existing column
      # use the composite key syntax even if it is only one column.
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
      def add_index(columns, opts = {})
        @operations << {:op => :add_index, :columns => Array(columns)}.merge(opts)
      end
      
      # Add a primary key to the DDL for the table.  See Generator#column
      # for the available options.
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
      def drop_column(name)
        @operations << {:op => :drop_column, :name => name}
      end
      
      # Remove a constraint from the DDL for the table.
      def drop_constraint(name)
        @operations << {:op => :drop_constraint, :name => name}
      end
      
      # Remove an index from the DDL for the table.
      def drop_index(columns, options={})
        @operations << {:op => :drop_index, :columns => Array(columns)}.merge(options)
      end

      # Modify a column's name in the DDL for the table.
      def rename_column(name, new_name, opts = {})
        @operations << {:op => :rename_column, :name => name, :new_name => new_name}.merge(opts)
      end
      
      # Modify a column's default value in the DDL for the table.
      def set_column_default(name, default)
        @operations << {:op => :set_column_default, :name => name, :default => default}
      end

      # Modify a column's type in the DDL for the table.
      def set_column_type(name, type, opts={})
        @operations << {:op => :set_column_type, :name => name, :type => type}.merge(opts)
      end
      
      # Modify a column's NOT NULL constraint.
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

