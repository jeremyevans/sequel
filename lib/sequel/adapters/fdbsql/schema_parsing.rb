module Sequel
  module Fdbsql
    # All the schema parsing like things, i.e. getting meta info about tables, etc.
    module SchemaParsing

      # Array of symbols specifying table names in the current database.
      # The dataset used is yielded to the block if one is provided,
      # otherwise, an array of symbols of table names is returned.
      #
      # Options:
      # :qualify :: Return the tables as Sequel::SQL::QualifiedIdentifier instances,
      #             using the schema the table is located in as the qualifier.
      # :schema :: The schema to search
      # :server :: The server to use
      def tables(opts=OPTS, &block)
        tables_or_views('TABLE', opts, &block)
      end


      # Array of symbols specifying view names in the current database.
      #
      # Options:
      # :qualify :: Return the views as Sequel::SQL::QualifiedIdentifier instances,
      #             using the schema the view is located in as the qualifier.
      # :schema :: The schema to search
      # :server :: The server to use
      def views(opts=OPTS, &block)
        tables_or_views('VIEW', opts, &block)
      end

      # Return primary key for the given table.
      def primary_key(table_name, opts=OPTS)
        quoted_table = quote_schema_table(table_name)
        Sequel.synchronize{return @primary_keys[quoted_table] if @primary_keys.has_key?(quoted_table)}
        # CURRENT_SCHEMA evaluates to the currently chosen schema
        schema = opts[:schema] ? opts[:schema] : Sequel.lit('CURRENT_SCHEMA')
        in_identifier = input_identifier_meth(opts[:dataset])
        out_identifier = output_identifier_meth(opts[:dataset])
        dataset = metadata_dataset.
          select(:kc__column_name).
          from(Sequel.as(:information_schema__key_column_usage, 'kc')).
          join(Sequel.as(:information_schema__table_constraints, 'tc'),
               tc__constraint_type: 'PRIMARY KEY',
               tc__table_name: :kc__table_name,
               tc__table_schema: :kc__table_schema,
               tc__constraint_name: :kc__constraint_name).
          filter(kc__table_name: in_identifier.call(table_name.to_s),
                 kc__table_schema: schema)
        value = dataset.map do |row|
          out_identifier.call(row.delete(:column_name))
        end
        value = case value.size
                  when 0 then nil
                  when 1 then value.first
                  else value
                end
        Sequel.synchronize{@primary_keys[quoted_table] = value}
      end

      # returns an array of column information with each column being of the form:
      # [:column_name, {:db_type=>"integer", :default=>nil, :allow_null=>false, :primary_key=>true, :type=>:integer}]
      def schema_parse_table(table_name, opts = {})
        out_identifier = output_identifier_meth(opts[:dataset])
        in_identifier = input_identifier_meth(opts[:dataset])
        # CURRENT_SCHEMA evaluates to the currently chosen schema
        schema = opts[:schema] ? opts[:schema] : Sequel.lit('CURRENT_SCHEMA')
        dataset = metadata_dataset.
          select(:c__column_name,
                 Sequel.as({:c__is_nullable => 'YES'}, 'allow_null'),
                 Sequel.as(:c__column_default, 'default'),
                 Sequel.as(:c__data_type, 'db_type'),
                 :c__numeric_scale,
                 Sequel.as({:tc__constraint_type => 'PRIMARY KEY'}, 'primary_key')).
          from(Sequel.as(:information_schema__key_column_usage, 'kc')).
          join(Sequel.as(:information_schema__table_constraints, 'tc'),
               tc__constraint_type: 'PRIMARY KEY',
               tc__table_name: :kc__table_name,
               tc__table_schema: :kc__table_schema,
               tc__constraint_name: :kc__constraint_name).
          right_outer_join(Sequel.as(:information_schema__columns, 'c'),
                           c__table_name: :kc__table_name,
                           c__table_schema: :kc__table_schema,
                           c__column_name: :kc__column_name).
          filter(c__table_name: in_identifier.call(table_name.to_s),
                 c__table_schema: schema)
        dataset.map do |row|
          row[:default] = nil if blank_object?(row[:default])
          row[:type] = schema_column_type(normalize_decimal_to_integer(row[:db_type], row[:numeric_scale]))
          [out_identifier.call(row.delete(:column_name)), row]
        end
      end

      def column_schema_normalize_default(default, type)
        # the default value returned by schema parsing is not escaped or quoted
        # in any way, it's just the value of the string
        # the base implementation assumes it would come back "'my ''default'' value'"
        # fdbsql returns "my 'default' value" (Not including double quotes for either)
        return default
      end

      def normalize_decimal_to_integer(type, scale)
        if (type == 'DECIMAL' and scale == 0)
          'integer'
        else
          type
        end
      end

      private

      def tables_or_views(type, opts, &block)
        schema = opts[:schema] ? opts[:schema] : Sequel.lit('CURRENT_SCHEMA')
        m = output_identifier_meth
        dataset = metadata_dataset.server(opts[:server]).select(:table_name).
          from(Sequel.qualify('information_schema','tables')).
          filter(table_schema: schema).
          filter(table_type: type)
        if block_given?
          yield(dataset)
        elsif opts[:qualify]
          dataset.select_append(:table_schema).map{|r| Sequel.qualify(m.call(r[:table_schema]), m.call(r[:table_name])) }
        else
          dataset.map{|r| m.call(r[:table_name])}
        end
      end

    end
  end
end
