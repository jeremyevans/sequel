module Sequel
  class Model
    module Associations
      class AssociationReflection < Hash
        RECIPROCAL_ASSOCIATIONS = {:many_to_one=>:one_to_many, :one_to_many=>:many_to_one, :many_to_many=>:many_to_many}
  
        def associated_class
          self[:class] ||= self[:class_name].constantize
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
                return self[:reciprocal] = association_ivar(assoc_reflect[:name])
              end
            end
          else
            key = self[:key]
            associated_class.all_association_reflections.each do |assoc_reflect|
              if assoc_reflect[:type] == reciprocal_type && assoc_reflect[:key] == key
                return self[:reciprocal] = association_ivar(assoc_reflect[:name])
              end
            end
          end
          self[:reciprocal] = nil
        end
  
        private
          # Name symbol of association instance variable
          def association_ivar(name)
            :"@#{name}"
          end
      end
    end
  end
end
