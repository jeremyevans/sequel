module Sequel::Model::Associations
  # Map of association type symbols to association reflection classes.
  ASSOCIATION_TYPES = {}

  # AssociationReflection is a Hash subclass that keeps information on Sequel::Model associations. It
  # provides methods to reduce internal code duplication.  It should not
  # be instantiated by the user.
  class AssociationReflection < Hash
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
  
    # Whether the dataset needs a primary key to function, true by default.
    def dataset_need_primary_key?
      true
    end

    # Whether to eagerly graph a lazy dataset, true by default.
    def eager_graph_lazy_dataset?
      true
    end

    # Whether the associated object needs a primary key to be added/removed,
    # false by default.
    def need_associated_primary_key?
      false
    end

    # Returns/sets the reciprocal association variable, if one exists
    def reciprocal
      return self[:reciprocal] if include?(:reciprocal)
      r_type = reciprocal_type
      key = self[:key]
      associated_class.all_association_reflections.each do |assoc_reflect|
        if assoc_reflect[:type] == r_type && assoc_reflect[:key] == key
          return self[:reciprocal] = assoc_reflect[:name]
        end
      end
      self[:reciprocal] = nil
    end

    # Whether the reciprocal of this  association returns an array of objects instead of a single object,
    # true by default.
    def reciprocal_array?
      true
    end

    # Name symbol for remove_all_ association method
    def remove_all_method
      :"remove_all_#{self[:name]}"
    end
  
    # Name symbol for remove_ association method
    def remove_method
      :"remove_#{self[:name].to_s.singularize}"
    end
  
    # Whether this association returns an array of objects instead of a single object,
    # true by default.
    def returns_array?
      true
    end

    # The columns to select when loading the association, nil by default.
    def select
      self[:select]
    end

    # By default, associations shouldn't set the reciprocal association to self.
    def set_reciprocal_to_self?
      false
    end

    # Name symbol for setter association method
    def setter_method
      :"#{self[:name]}="
    end

  end

  class ManyToOneAssociationReflection < AssociationReflection
    ASSOCIATION_TYPES[:many_to_one] = self

    # Whether the dataset needs a primary key to function, false for many_to_one associations.
    def dataset_need_primary_key?
      false
    end

    # Default foreign key name symbol for foreign key in current model's table that points to
    # the given association's table's primary key.
    def default_key
      :"#{self[:name]}_id"
    end
  
    # Whether to eagerly graph a lazy dataset, true for many_to_one associations
    # only if the key is nil.
    def eager_graph_lazy_dataset?
      self[:key].nil?
    end

    # The key to use for the key hash when eager loading
    def eager_loader_key
      self[:key]
    end

    # The column in the associated table that the key in the current table references.
    def primary_key
     self[:primary_key] ||= associated_class.primary_key
    end
  
    # Whether this association returns an array of objects instead of a single object,
    # false for a many_to_one association.
    def returns_array?
      false
    end

    private

    # The reciprocal type of a many_to_one association is a one_to_many association.
    def reciprocal_type
      :one_to_many
    end
  end

  class OneToManyAssociationReflection < AssociationReflection
    ASSOCIATION_TYPES[:one_to_many] = self

    # Default foreign key name symbol for key in associated table that points to
    # current table's primary key.
    def default_key
      :"#{self[:model].name.to_s.demodulize.underscore}_id"
    end

    # The key to use for the key hash when eager loading
    def eager_loader_key
      primary_key
    end

    # The column in the current table that the key in the associated table references.
    def primary_key
     self[:primary_key] ||= self[:model].primary_key
    end
  
    # One to many associations set the reciprocal to self.
    def set_reciprocal_to_self?
      true
    end

    # Whether the reciprocal of this  association returns an array of objects instead of a single object,
    # false for a one_to_many association.
    def reciprocal_array?
      false
    end

    private

    # The reciprocal type of a one_to_many association is a many_to_one association.
    def reciprocal_type
      :many_to_one
    end
  end

  class ManyToManyAssociationReflection < AssociationReflection
    ASSOCIATION_TYPES[:many_to_many] = self

    # Default name symbol for the join table.
    def default_join_table
      ([self[:class_name].demodulize, self[:model].name.to_s.demodulize]. \
        map{|i| i.pluralize.underscore}.sort.join('_')).to_sym
    end
  
    # Default foreign key name symbol for key in join table that points to
    # current table's primary key (or :left_primary_key column).
    def default_left_key
      :"#{self[:model].name.to_s.demodulize.underscore}_id"
    end

    # Default foreign key name symbol for foreign key in join table that points to
    # the association's table's primary key (or :right_primary_key column).
    def default_right_key
      :"#{self[:name].to_s.singularize}_id"
    end
  
    # The key to use for the key hash when eager loading
    def eager_loader_key
      self[:left_primary_key]
    end

    # Whether the associated object needs a primary key to be added/removed,
    # true for many_to_many associations.
    def need_associated_primary_key?
      true
    end

    # Returns/sets the reciprocal association variable, if one exists
    def reciprocal
      return self[:reciprocal] if include?(:reciprocal)
      left_key = self[:left_key]
      right_key = self[:right_key]
      join_table = self[:join_table]
      associated_class.all_association_reflections.each do |assoc_reflect|
        if assoc_reflect[:type] == :many_to_many && assoc_reflect[:left_key] == right_key \
           && assoc_reflect[:right_key] == left_key && assoc_reflect[:join_table] == join_table
          return self[:reciprocal] = assoc_reflect[:name]
        end
      end
      self[:reciprocal] = nil
    end

    # The primary key column to use in the associated table.
    def right_primary_key
      self[:right_primary_key] ||= associated_class.primary_key
    end

    # The columns to select when loading the association, associated_class.table_name.* by default.
    def select
     return self[:select] if include?(:select)
     self[:select] ||= associated_class.table_name.*
    end
  end
end
