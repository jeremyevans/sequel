# frozen_string_literal: true

require_relative 'constraint_validations'
require_relative 'class_table_inheritance'

module Sequel
  module Plugins
    # = Overview
    #
    # The class_table_inheritance_constraint_validations plugin extends the
    # constraint_validations plugin to work correctly with the
    # class_table_inheritance plugin. It ensures that constraint_validations
    # are loaded from all tables in the class table inheritance hierarchy,
    # not just the base table.
    #
    # = Example
    #
    # For example, with this hierarchy, where each model has its own table with
    # constraint validations:
    #
    #       Employee
    #      /        \
    #   Staff     Manager
    #                |
    #            Executive
    #
    #   # Loads constraint_validations from the employees table
    #   class Employee < Sequel::Model
    #     plugin :class_table_inheritance
    #     plugin :constraint_validations
    #     plugin :class_table_inheritance_constraint_validations
    #   end
    #
    #   # Loads constraint_validations from managers and employees tables
    #   class Manager < Employee
    #   end
    #
    #   # Loads constraint_validations from executives, managers, and
    #   # employees tables
    #   class Executive < Manager
    #   end
    #
    #   # Loads constraint_validations from staff and employees tables
    #   class Staff < Employee
    #   end
    module ClassTableInheritanceConstraintValidations
      def self.apply(model)
        unless ConstraintValidations::InstanceMethods > model && ClassTableInheritance::InstanceMethods > model
          raise Error, "must load the constraint_validations and class_table_inheritance plugins into #{model} before loading class_table_inheritance_constraint_validations plugin"
        end
      end

      module ClassMethods
        private

        def inherited(subclass)
          super

          # constraint_validations will parse_constraint_validations in the
          # classes after_set_dataset hook. That runs before cti_tables are
          # updated for subclasses in class_table_inheritance's inherited
          # so re-parsing them here.
          subclass.send(:parse_constraint_validations)
        end

        def parse_constraint_validations_dataset
          reflections = {}
          allow_missing_columns = db_schema.select{|col, sch| sch[:allow_null] == false && nil != sch[:default]}.map(&:first)
          hash = Sequel.synchronize{db.constraint_validations}
          cv = []
          ds = @dataset.with_quote_identifiers(false)
          cti_tables.each do |table_name|
            table_name = ds.literal(table_name)
            cv += (Sequel.synchronize{hash[table_name]} || []).map{|r| constraint_validation_array(r, reflections, allow_missing_columns)}
          end
          @constraint_validations = cv
          @constraint_validation_reflections = reflections
        end
      end
    end
  end
end
