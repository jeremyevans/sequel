module Sequel
  class Model
    module Associations
      # AssociationReflection is a Hash subclass that keeps information on Sequel::Model associations. It
      # provides methods to reduce internal code duplication.  It should not
      # be instantiated by the user.
      class AssociationReflection < Hash
        ASSOCIATION_TYPES = [:many_to_one, :one_to_many, :many_to_many]
        RECIPROCAL_ASSOCIATIONS = {:many_to_one=>:one_to_many, :one_to_many=>:many_to_one, :many_to_many=>:many_to_many}
  
        # Name symbol for _add_ internal association method
        def _add_method
          :"_add_#{self[:name].to_s.singularize}"
        end
      
        # Name symbol for _dataset association method
        def _dataset_method
          :"_#{self[:name]}_dataset"
        end
      
        # Name symbol for _remove_all internal association method
        def _remove_all_method
          :"_remove_all_#{self[:name]}"
        end
      
        # Name symbol for _remove_ internal association method
        def _remove_method
          :"_remove_#{self[:name].to_s.singularize}"
        end
      
        # Name symbol for setter association method
        def _setter_method
          :"_#{self[:name]}="
        end
      
        # Name symbol for add_ association method
        def add_method
          :"add_#{self[:name].to_s.singularize}"
        end
      
        # Name symbol for association method, the same as the name of the association.
        def association_method
          self[:name]
        end
      
        # The class associated to the current model class via this association
        def associated_class
          self[:class] ||= self[:class_name].constantize
        end
  
        # Name symbol for dataset association method
        def dataset_method
          :"#{self[:name]}_dataset"
        end
      
        # Name symbol for _helper internal association method
        def dataset_helper_method
          :"_#{self[:name]}_dataset_helper"
        end
      
        # Whether the dataset needs a primary key to function
        def dataset_need_primary_key?
          self[:type] != :many_to_one
        end

        # Name symbol for default join table
        def default_join_table
          ([self[:class_name].demodulize, self[:model].name.to_s.demodulize]. \
            map{|i| i.pluralize.underscore}.sort.join('_')).to_sym
        end
      
        # Default foreign key name symbol for key in associated table that points to
        # current table's primary key.
        def default_left_key
          :"#{self[:model].name.to_s.demodulize.underscore}_id"
        end

        # Default foreign key name symbol for foreign key in current model's table that points to
        # the given association's table's primary key.
        def default_right_key
          :"#{self[:type] == :many_to_one ? self[:name] : self[:name].to_s.singularize}_id"
        end
      
        # Whether to eagerly graph a lazy dataset
        def eager_graph_lazy_dataset?
          self[:type] != :many_to_one or self[:key].nil?
        end

        # The key to use for the key hash when eager loading
        def eager_loader_key
          self[:type] == :many_to_one ? self[:key] : self.primary_key
        end

        # Whether the associated object needs a primary key to be added/removed
        def need_associated_primary_key?
          self[:type] == :many_to_many
        end

        # The primary key used in the association
        def primary_key
         self[:primary_key] ||= associated_class.primary_key
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

        # Name symbol for remove_all_ association method
        def remove_all_method
          :"remove_all_#{self[:name]}"
        end
      
        # Name symbol for remove_ association method
        def remove_method
          :"remove_#{self[:name].to_s.singularize}"
        end
      
        # The columns to select when loading the association
        def select
         return self[:select] if include?(:select)
         self[:select] = self[:type] == :many_to_many ? associated_class.table_name.* : nil
        end

        # Whether to set the reciprocal to the current object when loading
        def set_reciprocal_to_self?
          self[:type] == :one_to_many
        end

        # Name symbol for setter association method
        def setter_method
          :"#{self[:name]}="
        end

        # Whether the association should return a single object or multiple objects.
        def single_associated_object?
          self[:type] == :many_to_one
        end
      end
    end
  end
end
