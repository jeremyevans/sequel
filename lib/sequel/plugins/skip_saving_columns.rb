# frozen-string-literal: true

module Sequel
  module Plugins
    # The skip_saving_columms plugin allows skipping specific columns when
    # saving.  By default, it skips columns that the database schema
    # indicates are generated columns:
    #
    #   # Assume id column, name column, and id2 generated column
    #   album = Album[1]
    #   album.id # => 1
    #   album.name # => 'X'
    #   album.id2 # => 2
    #   album.save
    #   # UPDATE album SET name = 'X' WHERE (id = 1)
    #
    # You can override which columns will be skipped:
    #
    #   Album.skip_saving_columns = [:name]
    #   album.save
    #   # UPDATE album SET id2 = 2 WHERE (id = 1)
    #
    # The skipping happens for all usage of Model#save and callers of it (e.g.
    # Model.create, Model.update).  When using the plugin, the only way to get
    # it to save a column marked for skipping is to explicitly specify it:
    #
    #   album.save(columns: [:name, :id2])
    #   album.save
    #   # UPDATE album SET name = 'X', id2 = 2 WHERE (id = 1)
    #
    # Usage:
    #
    #   # Support skipping saving columns in all Sequel::Model subclasses
    #   # (called before loading subclasses)
    #   Sequel::Model.plugin :skip_saving_columns
    #
    #   # Support skipping saving columns in the Album class
    #   Album.plugin :skip_saving_columns
    module SkipSavingColumns
      # Setup skipping of the generated columns for a model with an existing dataset.
      def self.configure(mod)
        mod.instance_exec do
          set_skip_saving_generated_columns if @dataset
        end
      end

      module ClassMethods
        # An array of column symbols for columns to skip when saving.
        attr_reader :skip_saving_columns

        # Over the default array of columns to skip.  Once overridden, future
        # changes to the class's dataset and future subclasses will automatically
        # use these overridden columns, instead of introspecting the database schema.
        def skip_saving_columns=(v) 
          @_skip_saving_columns_no_override = true
          @skip_saving_columns = v.dup.freeze
        end

        Plugins.after_set_dataset(self, :set_skip_saving_generated_columns)
        Plugins.inherited_instance_variables(self, :@skip_saving_columns=>:dup, :@_skip_saving_columns_no_override=>nil)

        private

        # If the skip saving columns has not been overridden, check the database
        # schema and automatically skip any generated columns.
        def set_skip_saving_generated_columns
          return if @_skip_saving_columns_no_override
          s = []
          db_schema.each do |k, v|
            s << k if v[:generated] 
          end
          @skip_saving_columns = s.freeze
          nil
        end
      end

      module InstanceMethods
        private

        # Skip the columns the model has marked to skip when inserting.
        def _insert_values
          _save_removed_skipped_columns(Hash[super])
        end

        # Skip the columns the model has marked to skip when updating
        # all columns.
        def _save_update_all_columns_hash
          _save_removed_skipped_columns(super)
        end

        # Skip the columns the model has marked to skip when updating
        # only changed columns.
        def _save_update_changed_colums_hash
          _save_removed_skipped_columns(super)
        end

        # Remove any columns the model has marked to skip when saving.
        def _save_removed_skipped_columns(hash)
          model.skip_saving_columns.each do |column|
            hash.delete(column)
          end

          hash
        end
      end
    end
  end
end
