module Sequel
  module Plugins
    # The nested_attributes plugin allows you to create, update, and delete
    # associated objects directly by calling a method on the current object.
    # Nested attributes are defined using the nested_attributes class method:
    #
    #   Artist.one_to_many :albums
    #   Artist.plugin :nested_attributes
    #   Artist.nested_attributes :albums
    #
    # The nested_attributes call defines a single method, <tt><i>association</i>_attributes=</tt>,
    # (e.g. <tt>albums_attributes=</tt>).  So if you have an Artist instance:
    #
    #   a = Artist.new(:name=>'YJM')
    #
    # You can create new album instances related to this artist:
    #
    #   a.albums_attributes = [{:name=>'RF'}, {:name=>'MO'}]
    #
    # Note that this doesn't send any queries to the database yet.  That doesn't happen till
    # you save the object:
    #
    #   a.save
    #
    # That will save the artist first, and then save both albums.  If either the artist
    # is invalid or one of the albums is invalid, none of the objects will be saved to the
    # database, and all related validation errors will be available in the artist's validation
    # errors.
    #
    # In addition to creating new associated objects, you can also update existing associated
    # objects.  You just need to make sure that the primary key field is filled in for the
    # associated object:
    #
    #   a.update(:albums_attributes => [{:id=>1, :name=>'T'}])
    #
    # Since the primary key field is filled in, the plugin will update the album with id 1 instead
    # of creating a new album.
    #
    # If you would like to delete the associated object instead of updating it, you add a _delete
    # entry to the hash:
    #
    #   a.update(:albums_attributes => [{:id=>1, :_delete=>true}])
    #
    # This will delete the related associated object from the database.  If you want to leave the
    # associated object in the database, but just remove it from the association, add a _remove
    # entry in the hash:
    #
    #   a.update(:albums_attributes => [{:id=>1, :_remove=>true}])
    #
    # The above example was for a one_to_many association, but the plugin also works similarly
    # for other association types.  For one_to_one and many_to_one associations, you need to
    # pass a single hash instead of an array of hashes.
    #
    # This plugin is mainly designed to make it easy to use on html forms, where a single form
    # submission can contained nested attributes (and even nested attributes of those attributes).
    # You just need to name your form inputs correctly:
    #
    #   artist[name]
    #   artist[albums_attributes][0][:name]
    #   artist[albums_attributes][1][:id]
    #   artist[albums_attributes][1][:name]
    #
    # Your web stack will probably parse that into a nested hash similar to:
    #
    #   {:artist=>{:name=>?, :albums_attributes=>{0=>{:name=>?}, 1=>{:id=>?, :name=>?}}}}
    #
    # Then you can do:
    #
    #   artist.update(params[:artist])
    #
    # To save changes to the artist, create the first album and associate it to the artist,
    # and update the other existing associated album.
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
        # * :fields - If provided, should be an Array or proc. If it is an array,
        #   restricts the fields allowed to be modified through the
        #   association_attributes= method to the specific fields given. If it is
        #   a proc, it will be called with the associated object and should return an
        #   array of the allowable fields.
        # * :limit - For *_to_many associations, a limit on the number of records
        #   that will be processed, to prevent denial of service attacks.
        # * :reject_if - A proc that is given each attribute hash before it is
        #   passed to its associated object. If the proc returns a truthy
        #   value, the attribute hash is ignored.
        # * :remove - Allow disassociation of nested records (can remove the associated
        #   object from the parent object, but not destroy the associated object).
        # * :strict - Kept for backward compatibility. Setting it to false is
        #   equivalent to setting :unmatched_pk to :ignore.
        # * :transform - A proc to transform attribute hashes before they are
        #   passed to associated object. Takes two arguments, the parent object and
        #   the attribute hash. Uses the return value as the new attribute hash.
        # * :unmatched_pk - Specify the action to be taken if a primary key is
        #   provided in a record, but it doesn't match an existing associated
        #   object. Set to :create to create a new object with that primary
        #   key, :ignore to ignore the record, or :raise to raise an error.
        #   The default is :raise.
        #
        # If a block is provided, it is used to set the :reject_if option.
        def nested_attributes(*associations, &block)
          include(self.nested_attributes_module ||= Module.new) unless nested_attributes_module
          opts = associations.last.is_a?(Hash) ? associations.pop : {}
          reflections = associations.map{|a| association_reflection(a) || raise(Error, "no association named #{a} for #{self}")}
          reflections.each do |r|
            r[:nested_attributes] = opts
            r[:nested_attributes][:unmatched_pk] ||= opts.delete(:strict) == false ? :ignore : :raise
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
          unless keys == reflection.associated_object_keys.map{|x| obj.send(x)}
            raise(Error, "Modifying association dependent key(s) when updating associated objects is not allowed")
          end
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
            associations[reflection[:name]] = obj

            # Because we are modifying the associations cache manually before the
            # setter is called, we still want to run the setter code even though
            # the cached value will be the same as the given value.
            @set_associated_object_if_same = true

            # Don't need to validate the object twice if :validate association option is not false
            # and don't want to validate it at all if it is false.
            if reflection[:type] == :many_to_one
              before_save_hook{send(reflection.setter_method, obj.save(:validate=>false))}
            else
              after_save_hook{send(reflection.setter_method, obj)}
            end
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

        # Remove the given associated object from the current object. If the
        # :destroy option is given, destroy the object after disassociating it
        # (unless destroying the object would automatically disassociate it).
        # Returns the object removed.
        def nested_attributes_remove(reflection, obj, opts={})
          if !opts[:destroy] || reflection.remove_before_destroy?
            before_save_hook do
              if reflection.returns_array?
                send(reflection.remove_method, obj)
              else
                send(reflection.setter_method, nil)
              end
            end
          end
          after_save_hook{obj.destroy} if opts[:destroy]
          obj
        end

        # Set the fields in the obj based on the association, only allowing
        # specific :fields if configured.
        def nested_attributes_set_attributes(reflection, obj, attributes)
          if fields = reflection[:nested_attributes][:fields]
            fields = fields.call(obj) if fields.respond_to?(:call)
            obj.set_only(attributes, fields)
          else
            obj.set(attributes)
          end
        end

        # Modify the associated object based on the contents of the attributes hash:
        # * If a :transform block was given to nested_attributes, use it to modify the attribute hash.
        # * If a block was given to nested_attributes, call it with the attributes and return immediately if the block returns true.
        # * If a primary key exists in the attributes hash and it matches an associated object:
        # ** If _delete is a key in the hash and the :destroy option is used, destroy the matching associated object.
        # ** If _remove is a key in the hash and the :remove option is used, disassociated the matching associated object.
        # ** Otherwise, update the matching associated object with the contents of the hash.
        # * If a primary key exists in the attributes hash but it does not match an associated object,
        #   either raise an error, create a new object or ignore the hash, depending on the :unmatched_pk option.
        # * If no primary key exists in the attributes hash, create a new object.
        def nested_attributes_setter(reflection, attributes)
          if a = reflection[:nested_attributes][:transform]
            attributes = a.call(self, attributes)
          end
          return if (b = reflection[:nested_attributes][:reject_if]) && b.call(attributes)
          modified!
          klass = reflection.associated_class
          sym_keys = Array(klass.primary_key)
          str_keys = sym_keys.map{|k| k.to_s}
          if (pk = attributes.values_at(*sym_keys)).all? || (pk = attributes.values_at(*str_keys)).all?
            pk = pk.map{|k| k.to_s}
            obj = Array(send(reflection[:name])).find{|x| Array(x.pk).map{|k| k.to_s} == pk}
          end
          if obj
            attributes = attributes.dup.delete_if{|k,v| str_keys.include? k.to_s}
            if reflection[:nested_attributes][:destroy] && klass.db.send(:typecast_value_boolean, attributes.delete(:_delete) || attributes.delete('_delete'))
              nested_attributes_remove(reflection, obj, :destroy=>true)
            elsif reflection[:nested_attributes][:remove] && klass.db.send(:typecast_value_boolean, attributes.delete(:_remove) || attributes.delete('_remove'))
              nested_attributes_remove(reflection, obj)
            else
              nested_attributes_update(reflection, obj, attributes)
            end
          elsif pk.all? && reflection[:nested_attributes][:unmatched_pk] != :create
            if reflection[:nested_attributes][:unmatched_pk] == :raise
              raise(Error, "no matching associated object with given primary key (association: #{reflection[:name]}, pk: #{pk})")
            end
          else
            nested_attributes_create(reflection, attributes)
          end
        end

        # Update the given object with the attributes, validating it when the
        # parent object is validated and saving it when the parent is saved.
        # Returns the object updated.
        def nested_attributes_update(reflection, obj, attributes)
          nested_attributes_update_attributes(reflection, obj, attributes)
          after_validation_hook{validate_associated_object(reflection, obj)}
          # Don't need to validate the object twice if :validate association option is not false
          # and don't want to validate it at all if it is false.
          after_save_hook{obj.save_changes(:validate=>false)}
          obj
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
