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
        out_identifier, in_identifier = identifier_convertors(opts)
        schema, table = schema_or_current_and_table(table_name, opts)
        dataset = metadata_dataset.
          select(:kc__column_name).
          from(Sequel.as(:information_schema__key_column_usage, 'kc')).
          join(Sequel.as(:information_schema__table_constraints, 'tc'),
               [:table_name, :table_schema, :constraint_name]).
          where(kc__table_name: in_identifier.call(table),
                kc__table_schema: schema,
                tc__constraint_type: 'PRIMARY KEY')
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
      def schema_parse_table(table, opts = {})
        out_identifier, in_identifier = identifier_convertors(opts)
        schema, table = schema_or_current_and_table(table, opts)
        dataset = metadata_dataset.
          select(:c__column_name,
                 Sequel.as({:c__is_nullable => 'YES'}, 'allow_null'),
                 :c__column_default___default,
                 :c__data_type___db_type,
                 :c__numeric_scale,
                 Sequel.as({:tc__constraint_type => 'PRIMARY KEY'}, 'primary_key')).
          from(Sequel.as(:information_schema__key_column_usage, 'kc')).
          join(Sequel.as(:information_schema__table_constraints, 'tc'),
               tc__constraint_type: 'PRIMARY KEY',
               tc__table_name: :kc__table_name,
               tc__table_schema: :kc__table_schema,
               tc__constraint_name: :kc__constraint_name).
          right_outer_join(Sequel.as(:information_schema__columns, 'c'),
                           [:table_name, :table_schema, :column_name]).
          where(c__table_name: in_identifier.call(table),
                c__table_schema: schema)
        dataset.map do |row|
          row[:default] = nil if blank_object?(row[:default])
          row[:type] = schema_column_type(normalize_decimal_to_integer(row[:db_type], row[:numeric_scale]))
          [out_identifier.call(row.delete(:column_name)), row]
        end
      end

      # Return full foreign key information, including
      # Postgres returns hash like:
      # {"b_e_fkey"=> {:name=>:b_e_fkey, :columns=>[:e], :on_update=>:no_action, :on_delete=>:no_action, :deferrable=>false, :table=>:a, :key=>[:c]}}
      def foreign_key_list(table, opts=OPTS)
        out_identifier, in_identifier = identifier_convertors(opts)
        schema, table = schema_or_current_and_table(table, opts)
        sql_table = in_identifier.call(table)
        columns_dataset = metadata_dataset.
          select(:tc__table_name___table_name,
                 :tc__table_schema___table_schema,
                 :tc__is_deferable___deferrable,
                 :kc__column_name___column_name,
                 :kc__constraint_schema___schema,
                 :kc__constraint_name___name,
                 :rc__update_rule___on_update,
                 :rc__delete_rule___on_delete).
          from(Sequel.as(:information_schema__table_constraints, 'tc')).
          join(Sequel.as(:information_schema__key_column_usage, 'kc'),
               [:constraint_schema, :constraint_name]).
          join(Sequel.as(:information_schema__referential_constraints, 'rc'),
               [:constraint_name, :constraint_schema]).
          where(tc__table_name: sql_table,
                tc__table_schema: schema,
                tc__constraint_type: 'FOREIGN KEY')

        keys_dataset = metadata_dataset.
          select(:rc__constraint_schema___schema,
                 :rc__constraint_name___name,
                 :kc__table_name___key_table,
                 :kc__column_name___key_column).
          from(Sequel.as(:information_schema__table_constraints, 'tc')).
          join(Sequel.as(:information_schema__referential_constraints, 'rc'),
               [:constraint_schema, :constraint_name]).
          join(Sequel.as(:information_schema__key_column_usage, 'kc'),
               kc__constraint_schema: :rc__unique_constraint_schema,
               kc__constraint_name: :rc__unique_constraint_name).
          where(tc__table_name: sql_table,
                tc__table_schema: schema,
                tc__constraint_type: 'FOREIGN KEY')
        foreign_keys = {}
        columns_dataset.each do |row|
          foreign_key = foreign_keys.fetch(row[:name]) do |key|
            foreign_keys[row[:name]] = row
            row[:name] = out_identifier.call(local_constraint_name(sql_table, row[:name]))
            row[:columns] = []
            row[:key] = []
            row
          end
          foreign_key[:columns] << out_identifier.call(row[:column_name])
        end
        keys_dataset.each do |row|
          foreign_key = foreign_keys[row[:name]]
          foreign_key[:table] = out_identifier.call(row[:key_table])
          foreign_key[:key] << out_identifier.call(row[:key_column])
        end
        foreign_keys.values
      end

      # Return indexes for the table
      # postgres returns:
      # {:blah_blah_index=>{:columns=>[:n], :unique=>true, :deferrable=>nil},
      #  :items_n_a_index=>{:columns=>[:n, :a], :unique=>false, :deferrable=>nil}}
      def indexes(table, opts=OPTS)
        out_identifier, in_identifier = identifier_convertors(opts)
        schema, table = schema_or_current_and_table(table, opts)
        dataset = metadata_dataset.
          select(:is__is_unique,
                 Sequel.as({:is__is_unique => 'YES'}, 'unique'),
                 :is__index_name,
                 :ic__column_name).
          from(Sequel.as(:information_schema__indexes, 'is')).
          join(Sequel.as(:information_schema__index_columns, 'ic'),
               ic__index_table_schema: :is__table_schema,
               ic__index_table_name: :is__table_name,
               ic__index_name: :is__index_name).
          where(is__table_schema: schema,
                is__table_name: in_identifier.call(table)).
          exclude(is__index_type: 'PRIMARY')
        indexes = {}
        dataset.each do |row|
          index = indexes.fetch(out_identifier.call(row[:index_name])) do |key|
            h = { :unique => row[:unique], :columns => [] }
            indexes[key] = h
            h
          end
          index[:columns] << out_identifier.call(row[:column_name])
        end
        indexes
      end

      def column_schema_normalize_default(default, type)
        # the default value returned by schema parsing is not escaped or quoted
        # in any way, it's just the value of the string
        # the base implementation assumes it would come back "'my ''default'' value'"
        # fdbsql returns "my 'default' value" (Not including double quotes for either)
        return default
      end

      private

      # The constraint name that we need for all other commands does not include
      # the table name, but the one returned by the information_schema tables
      # does include it. E.g. if we have the constraint "b.__fk_1" on table b
      # the correct (e.g. drop) is
      # `ALTER TABLE b DROP CONSTRAINT __fk_1`
      # See sql-layer:ReferentialConstraintsFactory
      def local_constraint_name(table_name, global_constraint_name)
        global_constraint_name[table_name.length+1..-1]
      end

      # If the given type is DECIMAL with scale 0, say that it's an integer
      def normalize_decimal_to_integer(type, scale)
        if (type == 'DECIMAL' and scale == 0)
          'integer'
        else
          type
        end
      end

      def tables_or_views(type, opts, &block)
        schema = opts[:schema] || Sequel.lit('CURRENT_SCHEMA')
        m = output_identifier_meth
        dataset = metadata_dataset.server(opts[:server]).select(:table_name).
          from(Sequel.qualify('information_schema','tables')).
          where(table_schema: schema,
                table_type: type)
        if block_given?
          yield(dataset)
        elsif opts[:qualify]
          dataset.select_append(:table_schema).map{|r| Sequel.qualify(m.call(r[:table_schema]), m.call(r[:table_name])) }
        else
          dataset.map{|r| m.call(r[:table_name])}
        end
      end

      def identifier_convertors(opts=OPTS)
        [output_identifier_meth(opts[:dataset]), input_identifier_meth(opts[:dataset])]
      end

      def schema_or_current_and_table(table, opts=OPTS)
        schema, table = schema_and_table(table)
        schema = opts.fetch(:schema, schema || Sequel.lit('CURRENT_SCHEMA'))
        [schema, table]
      end
    end
  end
end
