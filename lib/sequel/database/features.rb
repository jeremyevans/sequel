module Sequel
  class Database
    # ---------------------
    # :section: 9 - Methods that describe what the database supports
    # These methods all return booleans, with most describing whether or not the
    # database supprots a given feature.
    # ---------------------
    
    # Whether the database uses a global namespace for the index.  If
    # false, the indexes are going to be namespaced per table.
    def global_index_namespace?
      true
    end

    # Whether the database supports CREATE TABLE IF NOT EXISTS syntax,
    # false by default.
    def supports_create_table_if_not_exists?
      false
    end

    # Whether the database supports deferrable constraints, false
    # by default as few databases do.
    def supports_deferrable_constraints?
      false
    end

    # Whether the database supports deferrable foreign key constraints,
    # false by default as few databases do.
    def supports_deferrable_foreign_key_constraints?
      supports_deferrable_constraints?
    end

    # Whether the database supports DROP TABLE IF EXISTS syntax,
    # default is the same as #supports_create_table_if_not_exists?.
    def supports_drop_table_if_exists?
      supports_create_table_if_not_exists?
    end

    # Whether the database supports Database#foreign_key_list for
    # parsing foreign keys.
    def supports_foreign_key_parsing?
      respond_to?(:foreign_key_list)
    end

    # Whether the database supports Database#indexes for parsing indexes.
    def supports_index_parsing?
      respond_to?(:indexes)
    end

    # Whether the database supports partial indexes (indexes on a subset of a table).
    def supports_partial_indexes?
      false
    end

    # Whether the database and adapter support prepared transactions
    # (two-phase commit), false by default.
    def supports_prepared_transactions?
      false
    end

    # Whether the database and adapter support savepoints, false by default.
    def supports_savepoints?
      false
    end

    # Whether the database and adapter support savepoints inside prepared transactions
    # (two-phase commit), default is false.
    def supports_savepoints_in_prepared_transactions?
      supports_prepared_transactions? && supports_savepoints?
    end

    # Whether the database supports schema parsing via Database#schema.
    def supports_schema_parsing?
      respond_to?(:schema_parse_table, true)
    end

    # Whether the database supports Database#tables for getting list of tables.
    def supports_table_listing?
      respond_to?(:tables)
    end
    #
    # Whether the database supports Database#views for getting list of views.
    def supports_view_listing?
      respond_to?(:views)
    end

    # Whether the database and adapter support transaction isolation levels, false by default.
    def supports_transaction_isolation_levels?
      false
    end

    # Whether DDL statements work correctly in transactions, false by default.
    def supports_transactional_ddl?
      false
    end

    # Whether CREATE VIEW ... WITH CHECK OPTION is supported, false by default.
    def supports_views_with_check_option?
      !!view_with_check_option_support
    end

    # Whether CREATE VIEW ... WITH LOCAL CHECK OPTION is supported, false by default.
    def supports_views_with_local_check_option?
      view_with_check_option_support == :local
    end

    private

    # Whether the database supports combining multiple alter table
    # operations into a single query, false by default.
    def supports_combining_alter_table_ops?
      false
    end

    # Whether the database supports CREATE OR REPLACE VIEW.  If not, support
    # will be emulated by dropping the view first. false by default.
    def supports_create_or_replace_view?
      false
    end

    # Whether the database supports named column constraints. True
    # by default.  Those that don't support named column constraints
    # have to have column constraints converted to table constraints
    # if the column constraints have names.
    def supports_named_column_constraints?
      true
    end

    # Don't advertise support for WITH CHECK OPTION by default.
    def view_with_check_option_support
      nil
    end
  end
end
