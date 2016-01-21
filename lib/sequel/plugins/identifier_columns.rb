# frozen-string-literal: true

module Sequel
  module Plugins
    # The identifier_columns plugin makes Sequel automatically
    # handle column names containing 2 or more consecutive
    # underscores when creating or updating model objects.
    # By default, this doesn't work correctly in Sequel, as it
    # handles such symbols specially.
    #
    # This behavior isn't the default as it hurts performance,
    # and is rarely necessary.
    #
    # Usage:
    #
    #   # Make all model subclasses handle column names
    #   # with two or more underscores when saving
    #   Sequel::Model.plugin :identifier_columns
    #
    #   # Make the Album class handle column names
    #   # with two or more underscores when saving
    #   Album.plugin :identifier_columns
    module IdentifierColumns
      module InstanceMethods
        private

        # Use identifiers for value hash keys when inserting.
        def _insert_values
          identifier_hash(super)
        end

        # Use identifiers for value hash keys when updating.
        def _update_without_checking(columns)
          super(identifier_hash(columns))
        end

        # Convert the given columns hash from symbol
        # keys to Sequel::SQL::Identifier keys.
        def identifier_hash(columns)
          h = {}
          columns.each{|k,v| h[Sequel.identifier(k)] = v}
          h
        end
      end
    end
  end
end
