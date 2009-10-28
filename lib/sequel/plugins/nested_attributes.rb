module Sequel
  module Plugins
    # The nested_attributes plugin allows you to update attributes for associated
    # objects directly through the parent object, similar to ActiveRecord's
    # Nested Attributes feature.
    #
    # Nested attributes are created using the nested_attributes method:
    #
    #   Artist.one_to_many :albums
    #   Artist.nested_attributes :albums
    #   a = Artist.new(:name=>'YJM',
    #    :albums_attributes=>[{:name=>'RF'}, {:name=>'MO'}])
    #   # No database activity yet
    #
    #   a.save # Saves artist and both albums
    #   a.albums.map{|x| x.name} # ['RF', 'MO']
    module NestedAttributes
      # Depend on the instance_hooks plugin.
      def self.apply(model)
        model.plugin(:instance_hooks)
      end
      
      module ClassMethods
        # Module to store the nested_attributes setter methods, so they can
        # call be overridden and call super to get the default behavior
        attr_accessor :nested_attributes_module
        
        # Allow nested attributes to be set for the given associations.  Options:
        # * :destroy - Allow destruction of nested records.
        # * :fields - If provided, should be an Array.  Restricts the fields allowed to be
        #   modified through the association_attributes= method to the specific fields given.
        # * :limit - For *_to_many associations, a limit on the number of records
        #   that will be processed, to prevent denial of service attacks.
        # * :remove - Allow disassociation of nested records (can remove the associated
        #   object from the parent object, but not destroy the associated object).
        # * :strict - Set to false to not raise an error message if a primary key
        #   is provided in a record, but it doesn't match an existing associated
        #   object.
        #
        # If a block is provided, it is passed each nested attribute hash.  If
        # the hash should be ignored, the block should return anything except false or nil.
        def nested_attributes(*associations, &block)
          include(self.nested_attributes_module ||= Module.new) unless nested_attributes_module
          opts = associations.last.is_a?(Hash) ? associations.pop : {}
          reflections = associations.map{|a| association_reflection(a) || raise(Error, "no association named #{a} for #{self}")}
          reflections.each do |r|
            r[:nested_attributes] = opts
            r[:nested_attributes][:reject_if] ||= block
            def_nested_attribute_method(r)
          end
        end
        
        private
        
        # Add a nested attribute setter method to a module included in the
        # class.
        def def_nested_attribute_method(reflection)
          nested_attributes_module.class_eval do
            if reflection.returns_array?
              define_method("#{reflection[:name]}_attributes=") do |array|
                nested_attributes_list_setter(reflection, array)
              end
            else
             define_method("#{reflection[:name]}_attributes=") do |h|
                nested_attributes_setter(reflection, h)
              end
            end
          end
        end
      end
      
      module InstanceMethods
        private
        
        # Check that the keys related to the association are not modified inside the block.  Does
        # not use an ensure block, so callers should be careful.
        def nested_attributes_check_key_modifications(reflection, obj)
          keys = reflection.associated_object_keys.map{|x| obj.send(x)}
          yield
          raise(Error, "Modifying association dependent key(s) when updating associated objects is not allowed") unless keys == reflection.associated_object_keys.map{|x| obj.send(x)}
        end
        
        # Create a new associated object with the given attributes, validate
        # it when the parent is validated, and save it when the object is saved.
        # Returns the object created.
        def nested_attributes_create(reflection, attributes)
          obj = reflection.associated_class.new
          nested_attributes_set_attributes(reflection, obj, attributes)
          after_validation_hook{validate_associated_object(reflection, obj)}
          if reflection.returns_array?
            send(reflection[:name]) << obj
            after_save_hook{send(reflection.add_method, obj)}
          else
            # Don't need to validate the object twice if :validate association option is not false
            # and don't want to validate it at all if it is false.
            before_save_hook{send(reflection.setter_method, obj.save(:validate=>false))}
          end
          obj
        end
        
        # Find an associated object with the matching pk.  If a matching option
        # is not found and the :strict option is not false, raise an Error.
        def nested_attributes_find(reflection, pk)
          pk = pk.to_s
          unless obj = Array(associated_objects = send(reflection[:name])).find{|x| x.pk.to_s == pk}
            raise(Error, 'no associated object with that primary key does not exist') unless reflection[:nested_attributes][:strict] == false
          end
          obj
        end
        
        # Take an array or hash of attribute hashes and set each one individually.
        # If a hash is provided it, sort it by key and then use the values.
        # If there is a limit on the nested attributes for this association,
        # make sure the length of the attributes_list is not greater than the limit.
        def nested_attributes_list_setter(reflection, attributes_list)
          attributes_list = attributes_list.sort_by{|x| x.to_s}.map{|k,v| v} if attributes_list.is_a?(Hash)
          if (limit = reflection[:nested_attributes][:limit]) && attributes_list.length > limit
            raise(Error, "number of nested attributes (#{attributes_list.length}) exceeds the limit (#{limit})")
          end
          attributes_list.each{|a| nested_attributes_setter(reflection, a)}
        end
        
        # Remove the matching associated object from the current object.
        # If the :destroy option is given, destroy the object after disassociating it.
        # Returns the object removed, if it exists.
        def nested_attributes_remove(reflection, pk, opts={})
          if obj = nested_attributes_find(reflection, pk)
            before_save_hook do
              if reflection.returns_array?
                send(reflection.remove_method, obj)
              else
                send(reflection.setter_method, nil)
              end
            end
            after_save_hook{obj.destroy} if opts[:destroy]
            obj
          end
        end
        
        # Set the fields in the obj based on the association, only allowing
        # specific :fields if configured.
        def nested_attributes_set_attributes(reflection, obj, attributes)
          if fields = reflection[:nested_attributes][:fields]
            obj.set_only(attributes, fields)
          else
            obj.set(attributes)
          end
        end

        # Modify the associated object based on the contents of the attribtues hash:
        # * If a block was given to nested_attributes, call it with the attributes and return immediately if the block returns true.
        # * If no primary key exists in the attributes hash, create a new object.
        # * If _delete is a key in the hash and the :destroy option is used, destroy the matching associated object.
        # * If _remove is a key in the hash and the :remove option is used, disassociated the matching associated object.
        # * Otherwise, update the matching associated object with the contents of the hash.
        def nested_attributes_setter(reflection, attributes)
          return if (b = reflection[:nested_attributes][:reject_if]) && b.call(attributes)
          modified!
          klass = reflection.associated_class
          if pk = attributes.delete(klass.primary_key) || attributes.delete(klass.primary_key.to_s)
            if klass.db.send(:typecast_value_boolean, attributes[:_delete] || attributes['_delete']) && reflection[:nested_attributes][:destroy]
              nested_attributes_remove(reflection, pk, :destroy=>true)
            elsif klass.db.send(:typecast_value_boolean, attributes[:_remove] || attributes['_remove']) && reflection[:nested_attributes][:remove]
              nested_attributes_remove(reflection, pk)
            else
              nested_attributes_update(reflection, pk, attributes)
            end
          else
            nested_attributes_create(reflection, attributes)
          end
        end
        
        # Update the matching associated object with the attributes,
        # validating it when the parent object is validated and saving it
        # when the parent is saved.
        # Returns the object updated, if it exists.
        def nested_attributes_update(reflection, pk, attributes)
          if obj = nested_attributes_find(reflection, pk)
            nested_attributes_update_attributes(reflection, obj, attributes)
            after_validation_hook{validate_associated_object(reflection, obj)}
            # Don't need to validate the object twice if :validate association option is not false
            # and don't want to validate it at all if it is false.
            after_save_hook{obj.save(:validate=>false)}
            obj
          end
        end

        # Update the attributes for the given object related to the current object through the association.
        def nested_attributes_update_attributes(reflection, obj, attributes)
          nested_attributes_check_key_modifications(reflection, obj) do
            nested_attributes_set_attributes(reflection, obj, attributes)
          end
        end

        # Validate the given associated object, adding any validation error messages from the
        # given object to the parent object.
        def validate_associated_object(reflection, obj)
          return if reflection[:validate] == false
          association = reflection[:name]
          obj.errors.full_messages.each{|m| errors.add(association, m)} unless obj.valid?
        end
      end
    end
  end
end
