module Sequel
  class Model
    module Associations
      # AssociationReflection is a Hash subclass that keeps information on Sequel::Model associations. It
      # provides a few methods to reduce the amount of internal code duplication.  It should not
      # be instantiated by the user.
      class AssociationReflection < Hash
        RECIPROCAL_ASSOCIATIONS = {:many_to_one=>:one_to_many, :one_to_many=>:many_to_one, :many_to_many=>:many_to_many}
  
        # The class associated to the current model class via this association
        def associated_class
          self[:class] ||= self[:class_name].constantize
        end
  
        # The associated class's primary key (used for caching)
        def associated_primary_key
         self[:associated_primary_key] ||= associated_class.primary_key
        end
  
        # Returns/sets the reciprocal association variable, if one exists
        def reciprocal
          return self[:reciprocal] if include?(:reciprocal)
          reciprocal_type = RECIPROCAL_ASSOCIATIONS[self[:type]]
          if reciprocal_type == :many_to_many
            left_key = self[:left_key]
            right_key = self[:right_key]
            join_table = self[:join_table]
            associated_class.all_association_reflections.each do |assoc_reflect|
              if assoc_reflect[:type] == :many_to_many && assoc_reflect[:left_key] == right_key \
                 && assoc_reflect[:right_key] == left_key && assoc_reflect[:join_table] == join_table
                return self[:reciprocal] = assoc_reflect[:name]
              end
            end
          else
            key = self[:key]
            associated_class.all_association_reflections.each do |assoc_reflect|
              if assoc_reflect[:type] == reciprocal_type && assoc_reflect[:key] == key
                return self[:reciprocal] = assoc_reflect[:name]
              end
            end
          end
          self[:reciprocal] = nil
        end

        # The columns to select when loading the association
        def select
         self[:select] ||= associated_class.table_name.*
        end
      end
    end
  end
end
