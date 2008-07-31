module Sequel
  module MySQL
    # Methods shared by Database instances that connect to MySQL,
    # currently supported by the native and JDBC adapters.
    module DatabaseMethods
      AUTO_INCREMENT = 'AUTO_INCREMENT'.freeze
      NOT_NULL = Sequel::Schema::SQL::NOT_NULL
      NULL = Sequel::Schema::SQL::NULL
      PRIMARY_KEY = Sequel::Schema::SQL::PRIMARY_KEY
      SQL_BEGIN = Sequel::Database::SQL_BEGIN
      SQL_COMMIT = Sequel::Database::SQL_COMMIT
      SQL_ROLLBACK = Sequel::Database::SQL_ROLLBACK
      TYPES = Sequel::Schema::SQL::TYPES
      UNIQUE = Sequel::Schema::SQL::UNIQUE
      UNSIGNED = Sequel::Schema::SQL::UNSIGNED
      
      # Use MySQL specific syntax for rename column, set column type, and
      # drop index cases.
      def alter_table_sql(table, op)
        type = type_literal(op[:type])
        type << '(255)' if type == 'varchar'
        case op[:op]
        when :rename_column
          "ALTER TABLE #{table} CHANGE COLUMN #{literal(op[:name])} #{literal(op[:new_name])} #{type}"
        when :set_column_type
          "ALTER TABLE #{table} CHANGE COLUMN #{literal(op[:name])} #{literal(op[:name])} #{type}"
        when :drop_index
          "DROP INDEX #{default_index_name(table, op[:columns])} ON #{table}"
        else
          super(table, op)
        end
      end
      
      # Use MySQL specific AUTO_INCREMENT text.
      def auto_increment_sql
        AUTO_INCREMENT
      end
      
      # Handle MySQL specific column syntax (not sure why).
      def column_definition_sql(column)
        if column[:type] == :check
          return constraint_definition_sql(column)
        end
        sql = "#{literal(column[:name].to_sym)} #{TYPES[column[:type]]}"
        column[:size] ||= 255 if column[:type] == :varchar
        elements = column[:size] || column[:elements]
        sql << literal(Array(elements)) if elements
        sql << UNSIGNED if column[:unsigned]
        sql << UNIQUE if column[:unique]
        sql << NOT_NULL if column[:null] == false
        sql << NULL if column[:null] == true
        sql << " DEFAULT #{literal(column[:default])}" if column.include?(:default)
        sql << PRIMARY_KEY if column[:primary_key]
        sql << " #{auto_increment_sql}" if column[:auto_increment]
        if column[:table]
          sql << ", FOREIGN KEY (#{literal(column[:name].to_sym)}) REFERENCES #{column[:table]}"
          sql << literal(Array(column[:key])) if column[:key]
          sql << " ON DELETE #{on_delete_clause(column[:on_delete])}" if column[:on_delete]
        end
        sql
      end
      
      # Handle MySQL specific index SQL syntax
      def index_definition_sql(table_name, index)
        index_name = index[:name] || default_index_name(table_name, index[:columns])
        unique = "UNIQUE " if index[:unique]
        case index[:type]
        when :full_text
          "CREATE FULLTEXT INDEX #{index_name} ON #{table_name} #{literal(index[:columns])}"
        when :spatial
          "CREATE SPATIAL INDEX #{index_name} ON #{table_name} #{literal(index[:columns])}"
        when nil
          "CREATE #{unique}INDEX #{index_name} ON #{table_name} #{literal(index[:columns])}"
        else
          "CREATE #{unique}INDEX #{index_name} ON #{table_name} #{literal(index[:columns])} USING #{index[:type]}"
        end
      end
      
      # Get version of MySQL server, used for determined capabilities.
      def server_version
        m = /(\d+)\.(\d+)\.(\d+)/.match(get(:version[]))
        @server_version ||= (m[1].to_i * 10000) + (m[2].to_i * 100) + m[3].to_i
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
      
      # Always quote identifiers for the schema parser dataset.
      def schema_ds_dataset
        ds = schema_utility_dataset.clone
        ds.quote_identifiers = true
        ds
      end
      
      # Allow other database schema's to be queried using the :database
      # option.  Allow all database's schema to be used by setting
      # the :database option to nil.  If the database option is not specified,
      # uses the currently connected database.
      def schema_ds_filter(table_name, opts)
        filt = super
        # Restrict it to the given or current database, unless specifically requesting :database = nil
        filt = SQL::BooleanExpression.new(:AND, filt, {:c__table_schema=>opts[:database] || database_name}) if opts[:database] || !opts.include?(:database)
        filt
      end

      # MySQL doesn't support table catalogs, so just join on schema and table name.
      def schema_ds_join(table_name, opts)
        [:information_schema__columns, {:table_schema => :table_schema, :table_name => :table_name}, :c]
      end
    end
  
    # Dataset methods shared by datasets that use MySQL databases.
    module DatasetMethods
      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      COMMA_SEPARATOR = ', '.freeze
      
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
      def delete_sql(opts = nil)
        sql = super
        opts = opts ? @opts.merge(opts) : @opts

        if order = opts[:order]
          sql << " ORDER BY #{expression_list(order)}"
        end
        if limit = opts[:limit]
          sql << " LIMIT #{limit}"
        end

        sql
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
      def join_table(type, table, expr=nil, table_alias=nil)
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
      
      # Override the default boolean values.
      def literal(v)
        case v
        when true
          BOOL_TRUE
        when false
          BOOL_FALSE
        else
          super
        end
      end
      
      # MySQL specific syntax for inserting multiple values at once.
      def multi_insert_sql(columns, values)
        columns = column_list(columns)
        values = values.map {|r| literal(Array(r))}.join(COMMA_SEPARATOR)
        ["INSERT INTO #{source_list(@opts[:from])} (#{columns}) VALUES #{values}"]
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
      def update_sql(values, opts = nil)
        sql = super
        opts = opts ? @opts.merge(opts) : @opts

        if order = opts[:order]
          sql << " ORDER BY #{expression_list(order)}"
        end
        if limit = opts[:limit]
          sql << " LIMIT #{limit}"
        end

        sql
      end
    end
  end
end
