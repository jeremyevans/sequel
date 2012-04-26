module Sequel
  module Plugins
    # The prepared_statements_with_pk plugin allows Dataset#with_pk for model datasets
    # to use prepared statements by extract the values of previously bound variables
    # using <tt>Dataset#unbind</tt>, and attempting to use a prepared statement if the
    # variables can be unbound correctly. See  +Unbinder+ for details about what types of
    # dataset filters can be unbound correctly.
    #
    # This plugin depends on the +prepared_statements+ plugin and should be considered unsafe.
    # Unbinding dataset values cannot be done correctly in all cases, and use of this plugin
    # in cases where not there are variables that are not unbound can lead to an denial of
    # service attack by allocating an arbitrary number of prepared statements.  You have been
    # warned.
    #
    # Usage:
    #
    #   # Make all model subclasses use prepared statements for Dataset#with_pk (called before loading subclasses)
    #   Sequel::Model.plugin :prepared_statements_with_pk
    #
    #   # Make the Album class use prepared statements for Dataset#with_pk
    #   Album.plugin :prepared_statements_with_pk
    module PreparedStatementsWithPk
      # Depend on the prepared_statements plugin
      def self.apply(model)
        model.plugin(:prepared_statements)
      end

      module ClassMethods
        private

        # Return a prepared statement that can be used to lookup a row given a dataset for the row matching
        # the primary key.
        def prepared_lookup_dataset(ds)
          cached_prepared_statement(:lookup_sql, ds.sql){prepare_statement(ds.filter(prepared_statement_key_array(primary_key).map{|k, v| [SQL::QualifiedIdentifier.new(ds.model.table_name, k), v]}), :first)}
        end
      end

      module DatasetMethods
        # Use a prepared statement to find a row with the matching primary key
        # inside this dataset.
        def with_pk(pk)
          begin
            ds, bv = unbind
          rescue UnbindDuplicate
            super
          else
            begin
              bv = bv.merge!(model.primary_key_hash(pk)){|k, v1, v2| ((v1 == v2) ? v1 : raise(UnbindDuplicate))}
            rescue UnbindDuplicate
              super
            else
              model.send(:prepared_lookup_dataset, ds).call(bv)
            end
          end
        end
      end
    end
  end
end
