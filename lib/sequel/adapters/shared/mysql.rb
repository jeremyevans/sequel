module Sequel
  module MySQL
    class << self
      # Set the default options used for CREATE TABLE
      attr_accessor :default_charset, :default_collate, :default_engine
    end

    # Methods shared by Database instances that connect to MySQL,
    # currently supported by the native and JDBC adapters.
    module DatabaseMethods
      AUTO_INCREMENT = 'AUTO_INCREMENT'.freeze
      CAST_TYPES = {String=>:CHAR, Integer=>:SIGNED, Time=>:DATETIME, DateTime=>:DATETIME, Numeric=>:DECIMAL, BigDecimal=>:DECIMAL, File=>:BINARY}
      PRIMARY = 'PRIMARY'.freeze
      
      # MySQL's cast rules are restrictive in that you can't just cast to any possible
      # database type.
      def cast_type_literal(type)
        CAST_TYPES[type] || super
      end

      # MySQL uses the :mysql database type
      def database_type
        :mysql
      end

      # Return a hash containing index information. Hash keys are index name symbols.
      # Values are subhashes with two keys, :columns and :unique.  The value of :columns
      # is an array of symbols of column names.  The value of :unique is true or false
      # depending on if the index is unique.
      #
      # Does not include the primary key index or indexes on partial keys.
      def indexes(table)
        indexes = {}
        remove_indexes = []
        m = output_identifier_meth
        im = input_identifier_meth
        metadata_dataset.with_sql("SHOW INDEX FROM ?", SQL::Identifier.new(im.call(table))).each do |r|
          name = r[:Key_name]
          next if name == PRIMARY
          name = m.call(name)
          remove_indexes << name if r[:Sub_part]
          i = indexes[name] ||= {:columns=>[], :unique=>r[:Non_unique] != 1}
          i[:columns] << m.call(r[:Column_name])
        end
        indexes.reject{|k,v| remove_indexes.include?(k)}
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
        m = output_identifier_meth
        metadata_dataset.with_sql('SHOW TABLES').server(opts[:server]).map{|r| m.call(r.values.first)}
      end
      
      # MySQL supports savepoints
      def supports_savepoints?
        true
      end

      # Changes the database in use by issuing a USE statement.  I would be
      # very careful if I used this.
      def use(db_name)
        disconnect
        @opts[:database] = db_name if self << "USE #{db_name}"
        @schemas = {}
        self
      end
      
      private
      
      # Use MySQL specific syntax for rename column, set column type, and
      # drop index cases.
      def alter_table_sql(table, op)
        case op[:op]
        when :add_column
          if related = op.delete(:table)
            sql = super(table, op)
            op[:table] = related
            [sql, "ALTER TABLE #{quote_schema_table(table)} ADD FOREIGN KEY (#{quote_identifier(op[:name])})#{column_references_sql(op)}"]
          else
            super(table, op)
          end
        when :rename_column, :set_column_type, :set_column_null, :set_column_default
          o = op[:op]
          opts = schema(table).find{|x| x.first == op[:name]}
          opts = opts ? opts.last.dup : {}
          opts[:name] = o == :rename_column ? op[:new_name] : op[:name]
          opts[:type] = o == :set_column_type ? op[:type] : opts[:db_type]
          opts[:null] = o == :set_column_null ? op[:null] : opts[:allow_null]
          opts[:default] = o == :set_column_default ? op[:default] : opts[:ruby_default]
          opts.delete(:default) if opts[:default] == nil
          "ALTER TABLE #{quote_schema_table(table)} CHANGE COLUMN #{quote_identifier(op[:name])} #{column_definition_sql(opts)}"
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
      
      # MySQL doesn't allow default values on text columns, so ignore if it the
      # generic text type is used
      def column_definition_sql(column)
        column.delete(:default) if column[:type] == File || (column[:type] == String && column[:text] == true)
        super
      end
      
      # MySQL doesn't handle references as column constraints, it must use a separate table constraint
      def column_references_column_constraint_sql(column)
        "#{", FOREIGN KEY (#{quote_identifier(column[:name])})" unless column[:type] == :check}#{column_references_sql(column)}"
      end
      
      # Use MySQL specific syntax for engine type and character encoding
      def create_table_sql(name, generator, options = {})
        engine = options.include?(:engine) ? options[:engine] : Sequel::MySQL.default_engine
        charset = options.include?(:charset) ? options[:charset] : Sequel::MySQL.default_charset
        collate = options.include?(:collate) ? options[:collate] : Sequel::MySQL.default_collate
        "#{super}#{" ENGINE=#{engine}" if engine}#{" DEFAULT CHARSET=#{charset}" if charset}#{" DEFAULT COLLATE=#{collate}" if collate}"
      end

      # MySQL folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on input.
      def identifier_input_method_default
        nil
      end
      
      # MySQL folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on output.
      def identifier_output_method_default
        nil
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
        "CREATE #{index_type}INDEX #{index_name}#{using} ON #{quote_schema_table(table_name)} #{literal(index[:columns])}"
      end
      
      # MySQL treats integer primary keys as autoincrementing.
      def schema_autoincrementing_primary_key?(schema)
        super and schema[:db_type] =~ /int/io
      end

      # Use the MySQL specific DESCRIBE syntax to get a table description.
      def schema_parse_table(table_name, opts)
        m = output_identifier_meth
        im = input_identifier_meth
        metadata_dataset.with_sql("DESCRIBE ?", SQL::Identifier.new(im.call(table_name))).map do |row|
          row.delete(:Extra)
          row[:allow_null] = row.delete(:Null) == 'YES'
          row[:default] = row.delete(:Default)
          row[:primary_key] = row.delete(:Key) == 'PRI'
          row[:default] = nil if blank_object?(row[:default])
          row[:db_type] = row.delete(:Type)
          row[:type] = schema_column_type(row[:db_type])
          [m.call(row.delete(:Field)), row]
        end
      end

      # MySQL has both datetime and timestamp classes, most people are going
      # to want datetime
      def type_literal_generic_datetime(column)
        :datetime
      end

      # MySQL has both datetime and timestamp classes, most people are going
      # to want datetime
      def type_literal_generic_time(column)
        column[:only_time] ? :time : :datetime
      end

      # MySQL doesn't have a true boolean class, so it uses tinyint(1)
      def type_literal_generic_trueclass(column)
        :'tinyint(1)'
      end
    end
  
    # Dataset methods shared by datasets that use MySQL databases.
    module DatasetMethods
      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      COMMA_SEPARATOR = ', '.freeze
      DELETE_CLAUSE_METHODS = Dataset.clause_methods(:delete, %w'from where order limit')
      INSERT_CLAUSE_METHODS = Dataset.clause_methods(:insert, %w'ignore into columns values on_duplicate_key_update')
      SELECT_CLAUSE_METHODS = Dataset.clause_methods(:select, %w'distinct columns from join where group having compounds order limit')
      UPDATE_CLAUSE_METHODS = Dataset.clause_methods(:update, %w'table set where order limit')
      
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

      # Adds full text filter
      def full_text_search(cols, terms, opts = {})
        filter(full_text_sql(cols, terms, opts))
      end
      
      # MySQL specific full text search syntax.
      def full_text_sql(cols, term, opts = {})
        "MATCH #{literal(Array(cols))} AGAINST (#{literal(Array(term).join(' '))}#{" IN BOOLEAN MODE" if opts[:boolean]})"
      end

      # MySQL allows HAVING clause on ungrouped datasets.
      def having(*cond, &block)
        _filter(:having, *cond, &block)
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
      
      # Sets up multi_insert or import to use INSERT IGNORE.
      # Useful if you have a unique key and want to just skip
      # inserting rows that violate the unique key restriction.
      #
      # Example:
      #
      # dataset.insert_ignore.multi_insert(
      #  [{:name => 'a', :value => 1}, {:name => 'b', :value => 2}]
      # )
      #
      # INSERT IGNORE INTO tablename (name, value) VALUES (a, 1), (b, 2)
      #
      def insert_ignore
        clone(:insert_ignore=>true)
      end
      
      # Sets up multi_insert or import to use ON DUPLICATE KEY UPDATE
      # If you pass no arguments, ALL fields will be
      # updated with the new values.  If you pass the fields you
      # want then ONLY those field will be updated.
      #
      # Useful if you have a unique key and want to update
      # inserting rows that violate the unique key restriction.
      #
      # Examples:
      #
      # dataset.on_duplicate_key_update.multi_insert(
      #  [{:name => 'a', :value => 1}, {:name => 'b', :value => 2}]
      # )
      #
      # INSERT INTO tablename (name, value) VALUES (a, 1), (b, 2)
      # ON DUPLICATE KEY UPDATE name=VALUES(name), value=VALUES(value)
      #
      # dataset.on_duplicate_key_update(:value).multi_insert(
      #  [{:name => 'a', :value => 1}, {:name => 'b', :value => 2}]
      # )
      #
      # INSERT INTO tablename (name, value) VALUES (a, 1), (b, 2)
      # ON DUPLICATE KEY UPDATE value=VALUES(value)
      #
      def on_duplicate_key_update(*args)
        clone(:on_duplicate_key_update => args)
      end

      # MySQL specific syntax for inserting multiple values at once.
      def multi_insert_sql(columns, values)
        [insert_sql(columns, LiteralString.new('VALUES ' + values.map {|r| literal(Array(r))}.join(COMMA_SEPARATOR)))]
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
      
      #  does not support DISTINCT ON
      def supports_distinct_on?
        false
      end

      # MySQL does not support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end
      
      # MySQL does support fractional timestamps in literal timestamps, but it
      # ignores them.  Also, using them seems to cause problems on 1.9.  Since
      # they are ignored anyway, not using them is probably best.
      def supports_timestamp_usecs?
        false
      end

      private

      # MySQL supports the ORDER BY and LIMIT clauses for DELETE statements
      def delete_clause_methods
        DELETE_CLAUSE_METHODS
      end

      # MySQL supports the IGNORE and ON DUPLICATE KEY UPDATE clauses for INSERT statements
      def insert_clause_methods
        INSERT_CLAUSE_METHODS
      end

      # MySQL doesn't use the SQL standard DEFAULT VALUES.
      def insert_columns_sql(sql, columns, values)
        if values.is_a?(Array) && values.empty?
          sql << " ()"
        else
          super
        end
      end

      # MySQL supports INSERT IGNORE INTO
      def insert_ignore_sql(sql)
        sql << " IGNORE" if opts[:insert_ignore]
      end

      # MySQL supports INSERT ... ON DUPLICATE KEY UPDATE
      def insert_on_duplicate_key_update_sql(sql)
        sql << on_duplicate_key_update_sql if opts[:on_duplicate_key_update]
      end

      def insert_values_sql(sql, columns, values)
        if values.is_a?(Array) && values.empty?
          sql << " VALUES ()"
        else
          super
        end
      end

      def limit_sql(sql)
        sql << " LIMIT #{@opts[:limit]}" if @opts[:limit]
      end
      alias delete_limit_sql limit_sql
      alias update_limit_sql limit_sql

      # Use 0 for false on MySQL
      def literal_false
        BOOL_FALSE
      end

      # Use 1 for true on MySQL
      def literal_true
        BOOL_TRUE
      end
      
      # MySQL specific syntax for ON DUPLICATE KEY UPDATE
      def on_duplicate_key_update_sql
        if update_cols = opts[:on_duplicate_key_update]
          update_vals = nil

          if update_cols.empty?
            update_cols = columns
          elsif update_cols.last.is_a?(Hash)
            update_vals = update_cols.last
            update_cols = update_cols[0..-2]
          end

          updating = update_cols.map{|c| "#{quote_identifier(c)}=VALUES(#{quote_identifier(c)})" }
          updating += update_vals.map{|c,v| "#{quote_identifier(c)}=#{literal(v)}" } if update_vals

          " ON DUPLICATE KEY UPDATE #{updating.join(COMMA_SEPARATOR)}"
        end
      end

      # MySQL does not support the SQL WITH clause for SELECT statements
      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end

      # MySQL supports the ORDER BY and LIMIT clauses for UPDATE statements
      def update_clause_methods
        UPDATE_CLAUSE_METHODS
      end
    end
  end
end
