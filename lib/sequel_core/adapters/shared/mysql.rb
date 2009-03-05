require 'sequel_core/adapters/utils/unsupported'

module Sequel
  module Schema
    module SQL
      # Keep default column_references_sql for add_foreign_key support
      alias default_column_references_sql column_references_sql
    end
  end
  module MySQL
    # Set the default options used for CREATE TABLE
    metaattr_accessor :default_charset, :default_collate, :default_engine

    # Methods shared by Database instances that connect to MySQL,
    # currently supported by the native and JDBC adapters.
    module DatabaseMethods
      AUTO_INCREMENT = 'AUTO_INCREMENT'.freeze
      NOT_NULL = Sequel::Schema::SQL::NOT_NULL
      NULL = Sequel::Schema::SQL::NULL
      PRIMARY_KEY = Sequel::Schema::SQL::PRIMARY_KEY
      TYPES = Sequel::Schema::SQL::TYPES.merge(DateTime=>'datetime', \
        TrueClass=>'tinyint', FalseClass=>'tinyint')
      UNIQUE = Sequel::Schema::SQL::UNIQUE
      UNSIGNED = Sequel::Schema::SQL::UNSIGNED
      
      # Use MySQL specific syntax for rename column, set column type, and
      # drop index cases.
      def alter_table_sql(table, op)
        case op[:op]
        when :add_column
          if related = op.delete(:table)
            sql = super(table, op)
            op[:table] = related
            [sql, "ALTER TABLE #{quote_schema_table(table)} ADD FOREIGN KEY (#{quote_identifier(op[:name])})#{default_column_references_sql(op)}"]
          else
            super(table, op)
          end
        when :rename_column
          "ALTER TABLE #{quote_schema_table(table)} CHANGE COLUMN #{quote_identifier(op[:name])} #{quote_identifier(op[:new_name])} #{type_literal(op)}"
        when :set_column_type
          "ALTER TABLE #{quote_schema_table(table)} CHANGE COLUMN #{quote_identifier(op[:name])} #{quote_identifier(op[:name])} #{type_literal(op)}"
        when :drop_index
          "#{drop_index_sql(table, op)} ON #{quote_schema_table(table)}"
        else
          super(table, op)
        end
      end
      
      # Use MySQL specific AUTO_INCREMENT text.
      def auto_increment_sql
        AUTO_INCREMENT
      end
      
      # Handle MySQL specific syntax for column references
      def column_references_sql(column)
        "#{", FOREIGN KEY (#{quote_identifier(column[:name])})" unless column[:type] == :check}#{super(column)}"
      end
      
      # Use MySQL specific syntax for engine type and character encoding
      def create_table_sql_list(name, columns, indexes = nil, options = {})
        options[:engine] = Sequel::MySQL.default_engine unless options.include?(:engine)
        options[:charset] = Sequel::MySQL.default_charset unless options.include?(:charset)
        options[:collate] = Sequel::MySQL.default_collate unless options.include?(:collate)
        sql = ["CREATE TABLE #{quote_schema_table(name)} (#{column_list_sql(columns)})#{" ENGINE=#{options[:engine]}" if options[:engine]}#{" DEFAULT CHARSET=#{options[:charset]}" if options[:charset]}#{" DEFAULT COLLATE=#{options[:collate]}" if options[:collate]}"]
        sql.concat(index_list_sql_list(name, indexes)) if indexes && !indexes.empty?
        sql
      end

      # Handle MySQL specific index SQL syntax
      def index_definition_sql(table_name, index)
        index_name = quote_identifier(index[:name] || default_index_name(table_name, index[:columns]))
        index_type = case index[:type]
        when :full_text
          "FULLTEXT "
        when :spatial
          "SPATIAL "
        else
          using = " USING #{index[:type]}" unless index[:type] == nil
          "UNIQUE " if index[:unique]
        end
        "CREATE #{index_type}INDEX #{index_name} ON #{quote_schema_table(table_name)} #{literal(index[:columns])}#{using}"
      end
      
      # Get version of MySQL server, used for determined capabilities.
      def server_version
        m = /(\d+)\.(\d+)\.(\d+)/.match(get(SQL::Function.new(:version)))
        @server_version ||= (m[1].to_i * 10000) + (m[2].to_i * 100) + m[3].to_i
      end
      
      # Return an array of symbols specifying table names in the current database.
      #
      # Options:
      # * :server - Set the server to use
      def tables(opts={})
        ds = self['SHOW TABLES'].server(opts[:server])
        ds.identifier_output_method = nil
        ds2 = dataset
        ds.map{|r| ds2.send(:output_identifier, r.values.first)}
      end
      
      # Changes the database in use by issuing a USE statement.  I would be
      # very careful if I used this.
      def use(db_name)
        disconnect
        @opts[:database] = db_name if self << "USE #{db_name}"
        @schemas = nil
        self
      end
      
      private
      
      # MySQL folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on input.
      def identifier_input_method_default
        nil
      end
      
      # MySQL folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on output.
      def identifier_output_method_default
        nil
      end

      # Use the MySQL specific DESCRIBE syntax to get a table description.
      def schema_parse_table(table_name, opts)
        ds = self["DESCRIBE ?", SQL::Identifier.new(table_name)]
        ds.identifier_output_method = nil
        ds2 = dataset
        ds.map do |row|
          row.delete(:Extra)
          row[:allow_null] = row.delete(:Null) == 'YES'
          row[:default] = row.delete(:Default)
          row[:primary_key] = row.delete(:Key) == 'PRI'
          row[:default] = nil if row[:default].blank?
          row[:db_type] = row.delete(:Type)
          row[:type] = schema_column_type(row[:db_type])
          [ds2.send(:output_identifier, row.delete(:Field)), row]
        end
      end

      # Override the standard type conversions with MySQL specific ones
      def type_literal_base(column)
        TYPES[column[:type]]
      end
    end
  
    # Dataset methods shared by datasets that use MySQL databases.
    module DatasetMethods
      include Dataset::UnsupportedIntersectExcept

      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      CAST_TYPES = {String=>:CHAR, Integer=>:SIGNED, Time=>:DATETIME, DateTime=>:DATETIME, Numeric=>:DECIMAL, BigDecimal=>:DECIMAL, File=>:BINARY}
      TIMESTAMP_FORMAT = "'%Y-%m-%d %H:%M:%S'".freeze
      COMMA_SEPARATOR = ', '.freeze
      
      # MySQL can't use the varchar type in a cast.
      def cast_sql(expr, type)
        "CAST(#{literal(expr)} AS #{CAST_TYPES[type] || db.send(:type_literal_base, :type=>type)})"
      end

      # MySQL specific syntax for LIKE/REGEXP searches, as well as
      # string concatenation.
      def complex_expression_sql(op, args)
        case op
        when :~, :'!~', :'~*', :'!~*', :LIKE, :'NOT LIKE', :ILIKE, :'NOT ILIKE'
          "(#{literal(args.at(0))} #{'NOT ' if [:'NOT LIKE', :'NOT ILIKE', :'!~', :'!~*'].include?(op)}#{[:~, :'!~', :'~*', :'!~*'].include?(op) ? 'REGEXP' : 'LIKE'} #{'BINARY ' if [:~, :'!~', :LIKE, :'NOT LIKE'].include?(op)}#{literal(args.at(1))})"
        when :'||'
          if args.length > 1
            "CONCAT(#{args.collect{|a| literal(a)}.join(', ')})"
          else
            literal(args.at(0))
          end
        else
          super(op, args)
        end
      end
      
      # MySQL supports ORDER and LIMIT clauses in DELETE statements.
      def delete_sql(opts = (defarg=true;nil))
        if defarg
          sql = super()
          opts = @opts
        else
          sql = super
          opts = opts ? @opts.merge(opts) : @opts
        end

        if order = opts[:order]
          sql << " ORDER BY #{expression_list(order)}"
        end
        if limit = opts[:limit]
          sql << " LIMIT #{limit}"
        end

        sql
      end

      # MySQL doesn't support DISTINCT ON
      def distinct(*columns)
        raise(Error, "DISTINCT ON not supported by MySQL") unless columns.empty?
        super
      end

      # MySQL specific full text search syntax.
      def full_text_search(cols, terms, opts = {})
        mode = opts[:boolean] ? " IN BOOLEAN MODE" : ""
        s = if Array === terms
          if mode.blank?
            "MATCH #{literal(Array(cols))} AGAINST #{literal(terms)}"
          else
            "MATCH #{literal(Array(cols))} AGAINST (#{literal(terms)[1...-1]}#{mode})"
          end
        else
          "MATCH #{literal(Array(cols))} AGAINST (#{literal(terms)}#{mode})"
        end
        filter(s)
      end

      # MySQL allows HAVING clause on ungrouped datasets.
      def having(*cond, &block)
        @opts[:having] = {}
        x = filter(*cond, &block)
      end
      
      # MySQL doesn't use the SQL standard DEFAULT VALUES.
      def insert_default_values_sql
        "INSERT INTO #{source_list(@opts[:from])} () VALUES ()"
      end

      # Transforms an CROSS JOIN to an INNER JOIN if the expr is not nil.
      # Raises an error on use of :full_outer type, since MySQL doesn't support it.
      def join_table(type, table, expr=nil, table_alias={})
        type = :inner if (type == :cross) && !expr.nil?
        raise(Sequel::Error, "MySQL doesn't support FULL OUTER JOIN") if type == :full_outer
        super(type, table, expr, table_alias)
      end
      
      # Transforms :natural_inner to NATURAL LEFT JOIN and straight to
      # STRAIGHT_JOIN.
      def join_type_sql(join_type)
        case join_type
        when :straight then 'STRAIGHT_JOIN'
        when :natural_inner then 'NATURAL LEFT JOIN'
        else super
        end
      end
      
      # MySQL specific syntax for inserting multiple values at once.
      def multi_insert_sql(columns, values)
        values = values.map {|r| literal(Array(r))}.join(COMMA_SEPARATOR)
        ["INSERT INTO #{source_list(@opts[:from])} (#{identifier_list(columns)}) VALUES #{values}"]
      end
      
      # MySQL uses the nonstandard ` (backtick) for quoting identifiers.
      def quoted_identifier(c)
        "`#{c}`"
      end
      
      # MySQL specific syntax for REPLACE (aka UPSERT, or update if exists,
      # insert if it doesn't).
      def replace_sql(*values)
        from = source_list(@opts[:from])
        if values.empty?
          "REPLACE INTO #{from} DEFAULT VALUES"
        else
          values = values[0] if values.size == 1
          
          # if hash or array with keys we need to transform the values
          if @transform && (values.is_a?(Hash) || (values.is_a?(Array) && values.keys))
            values = transform_save(values)
          end

          case values
          when Array
            if values.empty?
              "REPLACE INTO #{from} DEFAULT VALUES"
            else
              "REPLACE INTO #{from} VALUES #{literal(values)}"
            end
          when Hash
            if values.empty?
              "REPLACE INTO #{from} DEFAULT VALUES"
            else
              fl, vl = [], []
              values.each {|k, v| fl << literal(k.is_a?(String) ? k.to_sym : k); vl << literal(v)}
              "REPLACE INTO #{from} (#{fl.join(COMMA_SEPARATOR)}) VALUES (#{vl.join(COMMA_SEPARATOR)})"
            end
          when Dataset
            "REPLACE INTO #{from} #{literal(values)}"
          else
            if values.respond_to?(:values)
              replace_sql(values.values)
            else  
              "REPLACE INTO #{from} VALUES (#{literal(values)})"
            end
          end
        end
      end
      
      # MySQL supports ORDER and LIMIT clauses in UPDATE statements.
      def update_sql(values, opts = (defarg=true;nil))
        if defarg
          sql = super(values)
          opts = @opts
        else
          sql = super
          opts = opts ? @opts.merge(opts) : @opts
        end

        if order = opts[:order]
          sql << " ORDER BY #{expression_list(order)}"
        end
        if limit = opts[:limit]
          sql << " LIMIT #{limit}"
        end

        sql
      end

      private

      # Use MySQL Timestamp format
      def literal_datetime(v)
        v.strftime(TIMESTAMP_FORMAT)
      end

      # Use 0 for false on MySQL
      def literal_false
        BOOL_FALSE
      end

      # Use MySQL Timestamp format
      def literal_time(v)
        v.strftime(TIMESTAMP_FORMAT)
      end

      # Use 1 for true on MySQL
      def literal_true
        BOOL_TRUE
      end
    end
  end
end
