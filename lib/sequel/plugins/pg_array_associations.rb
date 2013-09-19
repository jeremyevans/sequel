module Sequel
  extension :pg_array, :pg_array_ops

  module Plugins
    # This plugin allows you to create associations where the foreign keys
    # are stored in a PostgreSQL array column in one of the tables.  The
    # model with the table containing the array column has a
    # pg_array_to_many association to the associated model, and the
    # model with the table containing the primary key referenced by
    # elements in the array column has a many_to_pg_array association
    # to the associated model.
    #
    #   # Database schema:
    #   #   tags                albums
    #   #   :id (int4) <--\    :id
    #   #   :name          \-- :tag_ids (int4[])
    #   #                      :name
    #
    #   class Album
    #     plugin :pg_array_associations
    #     pg_array_to_many :tags
    #   end
    #   class Tag
    #     plugin :pg_array_associations
    #     many_to_pg_array :albums
    #   end
    #
    # These association types work similarly to Sequel's other association
    # types, so you can use them as you would any other association. Unlike
    # other associations, they do not support composite keys.
    #
    # One thing that is different is that the modification methods for
    # pg_array_to_many associations do not affect the database, since they
    # operate purely on the receiver.  For example:
    #
    #   album = Album[1]
    #   album.add_tag(Tag[2])
    #
    # does not save the album.  This allows you to call add_tag repeatedly
    # and the save after to combine all changes into a single query.  Note
    # that the many_to_pg_array association modification methods do save, so:
    #
    #   tag = Tag[2]
    #   tag.add_album(Album[1])
    #
    # will save the changes to the album.
    #
    # They support some additional options specific to this plugin:
    #
    # :array_type :: This allows you to specify the type of the array.  This
    #                is only necessary to set in very narrow circumstances,
    #                such as when this plugin needs to create an array type,
    #                and typecasting is turned off or not setup correctly
    #                for the model object.
    # :save_after_modify :: For pg_array_to_many associations, this makes the
    #                       the modification methods save the current object,
    #                       so they operate more similarly to the one_to_many
    #                       and many_to_many association modification methods.
    # :uniq :: Similar to many_to_many associations, this can be used to
    #          make sure the returned associated object array has uniq values.
    #
    # Note that until PostgreSQL gains the ability to enforce foreign key
    # constraints in array columns, this plugin is not recommended for
    # production use unless you plan on emulating referential integrity
    # constraints via triggers.
    #
    # This plugin should work on all supported PostgreSQL versions, except
    # the remove_all modification method for many_to_pg_array associations, which
    # requires the array_remove method added in PostgreSQL 9.3.
    module PgArrayAssociations
      # The AssociationReflection subclass for many_to_pg_array associations.
      class ManyToPgArrayAssociationReflection < Sequel::Model::Associations::AssociationReflection
        Sequel::Model::Associations::ASSOCIATION_TYPES[:many_to_pg_array] = self

        # The array column in the associated model containing foreign keys to
        # the current model.
        def associated_object_keys
          [self[:key]]
        end

        # many_to_pg_array associations can have associated objects as long as they have
        # a primary key.
        def can_have_associated_objects?(obj)
          obj.send(self[:primary_key])
        end

        # Assume that the key in the associated table uses a version of the current
        # model's name suffixed with _ids.
        def default_key
          :"#{underscore(demodulize(self[:model].name))}_ids"
        end

        # The hash key to use for the eager loading predicate (left side of IN (1, 2, 3))
        def predicate_key
          cached_fetch(:predicate_key){qualify_assoc(self[:key_column])}
        end

        # The column in the current table that the keys in the array column in the
        # associated table reference.
        def primary_key
          self[:primary_key]
        end

        # Destroying the associated object automatically removes the association,
        # since the association is stored in the associated object.
        def remove_before_destroy?
          false
        end

        private

        # Only consider an association as a reciprocal if it has matching keys
        # and primary keys.
        def reciprocal_association?(assoc_reflect)
          super && self[:key] == assoc_reflect[:key] && primary_key == assoc_reflect.primary_key
        end

        def reciprocal_type
          :pg_array_to_many
        end
      end

      # The AssociationReflection subclass for pg_array_to_many associations.
      class PgArrayToManyAssociationReflection < Sequel::Model::Associations::AssociationReflection
        Sequel::Model::Associations::ASSOCIATION_TYPES[:pg_array_to_many] = self

        # An array containing the primary key for the associated model.
        def associated_object_keys
          Array(primary_key)
        end

        # pg_array_to_many associations can only have associated objects if
        # the array field is not nil or empty.
        def can_have_associated_objects?(obj)
          v = obj.send(self[:key])
          v && !v.empty?
        end

        # pg_array_to_many associations do not need a primary key.
        def dataset_need_primary_key?
          false
        end

        # Use a default key name of *_ids, for similarity to other association types
        # that use *_id for single keys.
        def default_key
          :"#{singularize(self[:name])}_ids"
        end

        # A qualified version of the associated primary key.
        def predicate_key
          cached_fetch(:predicate_key){qualify_assoc(primary_key)}
        end

        # The primary key of the associated model.
        def primary_key
          cached_fetch(:primary_key){associated_class.primary_key}
        end

        # The method to call to get value of the primary key of the associated model.
        def primary_key_method
          cached_fetch(:primary_key_method){primary_key}
        end

        private

        # Only consider an association as a reciprocal if it has matching keys
        # and primary keys.
        def reciprocal_association?(assoc_reflect)
          super && self[:key] == assoc_reflect[:key] && primary_key == assoc_reflect.primary_key
        end

        def reciprocal_type
          :many_to_pg_array
        end
      end

      module ClassMethods
        # Create a many_to_pg_array association, for the case where the associated
        # table contains the array with foreign keys pointing to the current table.
        # See associate for options.
        def many_to_pg_array(name, opts=OPTS, &block)
          associate(:many_to_pg_array, name, opts, &block)
        end

        # Create a pg_array_to_many association, for the case where the current
        # table contains the array with foreign keys pointing to the associated table.
        # See associate for options.
        def pg_array_to_many(name, opts=OPTS, &block)
          associate(:pg_array_to_many, name, opts, &block)
        end

        private

        # Setup the many_to_pg_array-specific datasets, eager loaders, and modification methods.
        def def_many_to_pg_array(opts)
          name = opts[:name]
          model = self
          pk = opts[:eager_loader_key] = opts[:primary_key] ||= model.primary_key
          opts[:key] = opts.default_key unless opts.has_key?(:key)
          key = opts[:key]
          key_column = opts[:key_column] ||= opts[:key]
          opts[:after_load].unshift(:array_uniq!) if opts[:uniq]
          slice_range = opts.slice_range
          opts[:dataset] ||= lambda do
            opts.associated_dataset.where(Sequel.pg_array_op(opts.predicate_key).contains([send(pk)]))
          end
          opts[:eager_loader] ||= proc do |eo|
            id_map = eo[:id_map]
            rows = eo[:rows]
            rows.each do |object|
              object.associations[name] = []
            end

            klass = opts.associated_class
            ds = model.eager_loading_dataset(opts, klass.where(Sequel.pg_array_op(opts.predicate_key).overlaps(id_map.keys)), nil, eo[:associations], eo)
            ds.all do |assoc_record|
              if pks ||= assoc_record.send(key)
                pks.each do |pkv|
                  next unless objects = id_map[pkv]
                  objects.each do |object|
                    object.associations[name].push(assoc_record)
                  end
                end
              end
            end
            if slice_range
              rows.each{|o| o.associations[name] = o.associations[name][slice_range] || []}
            end
          end

          join_type = opts[:graph_join_type]
          select = opts[:graph_select]
          opts[:cartesian_product_number] ||= 1

          if opts.include?(:graph_only_conditions)
            conditions = opts[:graph_only_conditions]
            graph_block = opts[:graph_block]
          else
            conditions = opts[:graph_conditions]
            conditions = nil if conditions.empty?
            graph_block = proc do |j, lj, js|
              Sequel.pg_array_op(Sequel.deep_qualify(j, key_column)).contains([Sequel.deep_qualify(lj, opts.primary_key)])
            end

            if orig_graph_block = opts[:graph_block]
              pg_array_graph_block = graph_block
              graph_block = proc do |j, lj, js|
                Sequel.&(orig_graph_block.call(j,lj,js), pg_array_graph_block.call(j, lj, js))
              end
            end
          end

          opts[:eager_grapher] ||= proc do |eo|
            ds = eo[:self]
            ds = ds.graph(eager_graph_dataset(opts, eo), conditions, eo.merge(:select=>select, :join_type=>join_type, :qualify=>:deep, :from_self_alias=>ds.opts[:eager_graph][:master]), &graph_block)
            ds
          end

          def_association_dataset_methods(opts)

          unless opts[:read_only]
            validate = opts[:validate]

            array_type = opts[:array_type] ||= :integer
            adder = opts[:adder] || proc do |o|
              if array = o.send(key)
                array << send(pk)
              else
                o.send("#{key}=", Sequel.pg_array([send(pk)], array_type))
              end
              o.save(:validate=>validate) || raise(Sequel::Error, "invalid associated object, cannot save")
            end
            association_module_private_def(opts._add_method, opts, &adder)

            remover = opts[:remover] || proc do |o|
              if (array = o.send(key)) && !array.empty?
                array.delete(send(pk))
                o.save(:validate=>validate) || raise(Sequel::Error, "invalid associated object, cannot save")
              end
            end
            association_module_private_def(opts._remove_method, opts, &remover)

            clearer = opts[:clearer] || proc do
              opts.associated_dataset.where(Sequel.pg_array_op(key).contains([send(pk)])).update(key=>Sequel.function(:array_remove, key, send(pk)))
            end
            association_module_private_def(opts._remove_all_method, opts, &clearer)

            def_add_method(opts)
            def_remove_methods(opts)
          end
        end

        # Setup the pg_array_to_many-specific datasets, eager loaders, and modification methods.
        def def_pg_array_to_many(opts)
          name = opts[:name]
          model = self
          opts[:key] = opts.default_key unless opts.has_key?(:key)
          key = opts[:key]
          key_column = opts[:key_column] ||= key
          opts[:eager_loader_key] = nil
          opts[:after_load].unshift(:array_uniq!) if opts[:uniq]
          slice_range = opts.slice_range
          opts[:dataset] ||= lambda do
            opts.associated_dataset.where(opts.predicate_key=>send(key).to_a)
          end
          opts[:eager_loader] ||= proc do |eo|
            rows = eo[:rows]
            id_map = {}
            pkm = opts.primary_key_method
            rows.each do |object|
              object.associations[name] = []
              if associated_pks = object.send(key)
                associated_pks.each do |apk|
                  (id_map[apk] ||= []) << object
                end
              end
            end

            klass = opts.associated_class
            ds = model.eager_loading_dataset(opts, klass.where(opts.predicate_key=>id_map.keys), nil, eo[:associations], eo)
            ds.all do |assoc_record|
              if objects = id_map[assoc_record.send(pkm)]
                objects.each do |object|
                  object.associations[name].push(assoc_record)
                end
              end
            end
            if slice_range
              rows.each{|o| o.associations[name] = o.associations[name][slice_range] || []}
            end
          end

          join_type = opts[:graph_join_type]
          select = opts[:graph_select]
          opts[:cartesian_product_number] ||= 1

          if opts.include?(:graph_only_conditions)
            conditions = opts[:graph_only_conditions]
            graph_block = opts[:graph_block]
          else
            conditions = opts[:graph_conditions]
            conditions = nil if conditions.empty?
            graph_block = proc do |j, lj, js|
              Sequel.pg_array_op(Sequel.deep_qualify(lj, key_column)).contains([Sequel.deep_qualify(j, opts.primary_key)])
            end

            if orig_graph_block = opts[:graph_block]
              pg_array_graph_block = graph_block
              graph_block = proc do |j, lj, js|
                Sequel.&(orig_graph_block.call(j,lj,js), pg_array_graph_block.call(j, lj, js))
              end
            end
          end

          opts[:eager_grapher] ||= proc do |eo|
            ds = eo[:self]
            ds = ds.graph(eager_graph_dataset(opts, eo), conditions, eo.merge(:select=>select, :join_type=>join_type, :qualify=>:deep, :from_self_alias=>ds.opts[:eager_graph][:master]), &graph_block)
            ds
          end

          def_association_dataset_methods(opts)

          unless opts[:read_only]
            validate = opts[:validate]
            array_type = opts[:array_type] ||= :integer
            if opts[:save_after_modify]
              save_after_modify = proc do |obj|
                obj.save(:validate=>validate) || raise(Sequel::Error, "invalid associated object, cannot save")
              end
            end

            adder = opts[:adder] || proc do |o|
              opk = o.send(opts.primary_key)
              if array = send(key)
                modified!(key)
                array << opk
              else
                send("#{key}=", Sequel.pg_array([opk], array_type))
              end
              save_after_modify.call(self) if save_after_modify
            end
            association_module_private_def(opts._add_method, opts, &adder)

            remover = opts[:remover] || proc do |o|
              if (array = send(key)) && !array.empty?
                modified!(key)
                array.delete(o.send(opts.primary_key))
                save_after_modify.call(self) if save_after_modify
              end
            end
            association_module_private_def(opts._remove_method, opts, &remover)

            clearer = opts[:clearer] || proc do
              if (array = send(key)) && !array.empty?
                modified!(key)
                array.clear
                save_after_modify.call(self) if save_after_modify
              end
            end
            association_module_private_def(opts._remove_all_method, opts, &clearer)

            def_add_method(opts)
            def_remove_methods(opts)
          end
        end
      end

      module DatasetMethods
        private

        # Support filtering by many_to_pg_array associations using a subquery.
        def many_to_pg_array_association_filter_expression(op, ref, obj)
          pk = ref.qualify(model.table_name, ref.primary_key)
          key = ref[:key]
          expr = case obj
          when Sequel::Model
            if (assoc_pks = obj.send(key)) && !assoc_pks.empty?
              Sequel.expr(pk=>assoc_pks.to_a)
            end
          when Array
            if (assoc_pks = obj.map{|o| o.send(key)}.flatten.compact.uniq) && !assoc_pks.empty?
              Sequel.expr(pk=>assoc_pks)
            end
          when Sequel::Dataset
            Sequel.expr(pk=>obj.select{Sequel.pg_array_op(ref.qualify(obj.model.table_name, ref[:key_column])).unnest})
          end
          expr = Sequel::SQL::Constants::FALSE unless expr
          association_filter_handle_inversion(op, expr, [pk])
        end

        # Support filtering by pg_array_to_many associations using a subquery.
        def pg_array_to_many_association_filter_expression(op, ref, obj)
          key = ref.qualify(model.table_name, ref[:key_column])
          expr = case obj
          when Sequel::Model
            if pkv = obj.send(ref.primary_key_method)
              Sequel.pg_array_op(key).contains([pkv])
            end
          when Array
            if (pkvs = obj.map{|o| o.send(ref.primary_key_method)}.compact) && !pkvs.empty?
              Sequel.pg_array(key).overlaps(pkvs)
            end
          when Sequel::Dataset
            Sequel.function(:coalesce, Sequel.pg_array_op(key).overlaps(obj.select{array_agg(ref.qualify(obj.model.table_name, ref.primary_key))}), Sequel::SQL::Constants::FALSE)
          end
          expr = Sequel::SQL::Constants::FALSE unless expr
          association_filter_handle_inversion(op, expr, [key])
        end
      end
    end
  end
end
