module Sequel
  module Plugins
    # The identity_map plugin allows the user to create temporary identity maps
    # via the with_identity_map method, which takes a block.  Inside the block,
    # objects have a 1-1 correspondence with rows in the database.
    #
    # For example, the following is true, and wouldn't be true if you weren't
    # using the identity map:
    #   Sequel::Model.with_identity_map do
    #     Album.filter{(id > 0) & (id < 2)}.first.object_id == Album.first(:id=>1).object_id
    #   end
    #
    # In addition to providing a 1-1 correspondence, the identity_map plugin
    # also provides a cached looked up of records in two cases:
    # * Model.[] (e.g. Album[1])
    # * Model.many_to_one accessor methods (e.g. album.artist)
    #
    # If the object you are looking up, using one of those two methods, is already
    # in the identity map, the record is returned without a database query being
    # issued.
    #
    # Identity maps are thread-local and only persist for the duration of the block,
    # so they should only be considered as a possible performance enhancer.
    #
    # The identity_map plugin is not compatible with the eager loading in the +rcte_tree+ plugin.
    #
    # Usage:
    #
    #   # Use an identity map that will affect all model classes (called before loading subclasses)
    #   Sequel::Model.plugin :identity_map
    #
    #   # Use an identity map just for the Album class
    #   Album.plugin :identity_map
    #   # would need to do Album.with_identity_map{} to use the identity map
    module IdentityMap
      module ClassMethods
        # Override the default :eager_loader option for many_*_many associations to work
        # with an identity_map.  If the :eager_graph association option is used, you'll probably have to use
        # :uniq=>true on the current association amd :cartesian_product_number=>2 on the association
        # mentioned by :eager_graph, otherwise you'll end up with duplicates because the row proc will be
        # getting called multiple times for the same object.  If you do have duplicates and you use :eager_graph,
        # they'll probably be lost.  Making that work correctly would require changing a lot of the core
        # architecture, such as how graphing and eager graphing work.
        def associate(type, name, opts = {}, &block)
          if opts[:eager_loader]
            super
          elsif type == :many_to_many
            opts = super
            el = opts[:eager_loader] 
            model = self
            left_pk = opts[:left_primary_key]
            uses_lcks = opts[:uses_left_composite_keys]
            uses_rcks = opts[:uses_right_composite_keys]
            right = opts[:right_key]
            join_table = opts[:join_table]
            left = opts[:left_key]
            lcks = opts[:left_keys]
            left_key_alias = opts[:left_key_alias] ||= opts.default_associated_key_alias
            opts[:eager_loader] = proc do |eo|
              return el.call(eo) unless model.identity_map
              h = eo[:key_hash][left_pk]
              eo[:rows].each{|object| object.associations[name] = []}
              r = uses_rcks ? rcks.zip(opts.right_primary_keys) : [[right, opts.right_primary_key]]
              l = uses_lcks ? [[lcks.map{|k| SQL::QualifiedIdentifier.new(join_table, k)}, h.keys]] : [[left, h.keys]]

              # Replace the row proc to remove the left key alias before calling the previous row proc.
              # Associate the value of the left key alias with the associated object (through its object_id).
              # When loading the associated objects, lookup the left key alias value and associate the
              # associated objects to the main objects if the left key alias value matches the left primary key
              # value of the main object.
              # 
              # The deleting of the left key alias from the hash before calling the previous row proc
              # is necessary when an identity map is used, otherwise if the same associated object is returned more than
              # once for the association, it won't know which of current objects to associate it to.
              ds = opts.associated_class.inner_join(join_table, r + l)
              pr = ds.row_proc
              h2 = {}
              ds = if opts[:eager_graph]
                model.eager_loading_dataset(opts, ds, Array(opts.select), eo[:associations], eo).clone(:eager_graph_map=>[left_key_alias, h2])
              else
                ds.row_proc = proc do |hash|
                  hash_key = if uses_lcks
                    left_key_alias.map{|k| hash.delete(k)}
                  else
                    hash.delete(left_key_alias)
                  end
                  obj = pr.call(hash)
                  (h2[obj.object_id] ||= []) << hash_key
                  obj
                end
                model.eager_loading_dataset(opts, ds, Array(opts.select), eo[:associations], eo)
              end
              ds.all do |assoc_record|
                if hash_keys = h2.delete(assoc_record.object_id)
                  hash_keys.each do |hash_key|
                    if objects = h[hash_key]
                      objects.each{|object| object.associations[name].push(assoc_record)}
                    end
                  end
                end
              end
            end
            opts
          elsif type == :many_through_many
            opts = super
            el = opts[:eager_loader] 
            model = self
            left_pk = opts[:left_primary_key]
            left_key = opts[:left_key]
            uses_lcks = opts[:uses_left_composite_keys]
            left_keys = Array(left_key)
            left_key_alias = opts[:left_key_alias]
            opts[:eager_loader] = lambda do |eo|
              return el.call(eo) unless model.identity_map
              h = eo[:key_hash][left_pk]
              eo[:rows].each{|object| object.associations[name] = []}
              ds = opts.associated_class 
              opts.reverse_edges.each{|t| ds = ds.join(t[:table], Array(t[:left]).zip(Array(t[:right])), :table_alias=>t[:alias])}
              ft = opts[:final_reverse_edge]
              conds = uses_lcks ? [[left_keys.map{|k| SQL::QualifiedIdentifier.new(ft[:table], k)}, h.keys]] : [[left_key, h.keys]]

              # See above comment in many_to_many eager_loader
              ds = ds.join(ft[:table], Array(ft[:left]).zip(Array(ft[:right])) + conds, :table_alias=>ft[:alias])
              pr = ds.row_proc
              h2 = {}
              ds = if opts[:eager_graph]
                model.eager_loading_dataset(opts, ds, Array(opts.select), eo[:associations], eo).clone(:eager_graph_map=>[left_key_alias, h2])
              else
                ds.row_proc = proc do |hash|
                  hash_key = if uses_lcks
                    left_key_alias.map{|k| hash.delete(k)}
                  else
                    hash.delete(left_key_alias)
                  end
                  obj = pr.call(hash)
                  (h2[obj.object_id] ||= []) << hash_key
                  obj
                end
                model.eager_loading_dataset(opts, ds, Array(opts.select), eo[:associations], eo)
              end
              ds.all do |assoc_record|
                if hash_keys = h2.delete(assoc_record.object_id)
                  hash_keys.each do |hash_key|
                    if objects = h[hash_key]
                      objects.each{|object| object.associations[name].push(assoc_record)}
                    end
                  end
                end
              end
            end
            opts
          else
            super
          end
        end
          
        # Returns the current thread-local identity map.  Should be a hash if
        # there is an active identity map, and nil otherwise.
        def identity_map
          Thread.current[:sequel_identity_map]
        end

        # The identity map key for an object of the current class with the given pk.
        # May not always be correct for a class which uses STI.
        def identity_map_key(pk)
          "#{self}:#{pk ? Array(pk).join(',') : "nil:#{rand}"}"
        end

        # If the identity map is in use, check it for a current copy of the object.
        # If a copy does not exist, create a new object and add it to the identity map.
        # If a copy exists, add any values in the given row that aren't currently
        # in the object to the object's values.  This allows you to only request
        # certain fields in an initial query, make modifications to some of those
        # fields and request other, potentially overlapping fields in a new query,
        # and not have the second query override fields you modified.
        def load(row)
          return super unless idm = identity_map
          if o = idm[identity_map_key(Array(primary_key).map{|x| row[x]})]
            o.merge_db_update(row)
          else
            o = super
            idm[identity_map_key(o.pk)] = o
          end
          o
        end

        # Take a block and inside that block use an identity map to ensure a 1-1
        # correspondence of objects to the database row they represent.
        def with_identity_map
          return yield if identity_map
          begin
            self.identity_map = {}
            yield
          ensure
            self.identity_map = nil
          end
        end

        private

        # Set the thread local identity map to the given value.
        def identity_map=(v)
          Thread.current[:sequel_identity_map] = v
        end

        # Check the current identity map if it exists for the object with
        # the matching pk.  If one is found, return it, otherwise call super.
        def primary_key_lookup(pk)
          (idm = identity_map and o = idm[identity_map_key(pk)]) ? o : super
        end
      end

      module InstanceMethods
        # Remove instances from the identity map cache if they are deleted.
        def delete
          super
          if idm = model.identity_map
            idm.delete(model.identity_map_key(pk))
          end
          self
        end

        # Merge the current values into the values provided in the row, ensuring
        # that current values are not overridden by new values.
        def merge_db_update(row)
          @values = row.merge(@values)
        end

        private

        # The primary keys values of the associated object, given the foreign
        # key columns(s).
        def _associated_object_pk(fk)
          fk.is_a?(Array) ? fk.map{|c| send(c)} : send(fk)
        end

        # If the association is a many_to_one and it has a :key option and the
        # key option has a value and the association uses the primary key of
        # the associated class as the :primary_key option, check the identity
        # map for the associated object and return it if present.
        def _load_associated_objects(opts, dynamic_opts={})
          klass = opts.associated_class
          if !dynamic_opts[:callback] && klass.respond_to?(:identity_map) && idm = klass.identity_map and opts[:type] == :many_to_one and opts.primary_key == klass.primary_key and
           opts[:key] and pk = _associated_object_pk(opts[:key]) and o = idm[klass.identity_map_key(pk)]
            o
          else
            super
          end
        end
      end

      module DatasetMethods
        # If eagerly loading via +eager+ a many_*_many association that
        # has an :eager_graph option, and there is a current identity_map,
        # use the custom EagerGraphLoader that works with that case.
        def eager_graph_build_associations(hashes)
          if opts[:eager_graph_map] && model.identity_map
            hashes.replace(EagerGraphLoader.new(self).load(hashes))
          else
            super
          end
        end
      end

      # Specialized EagerGraphLoader that updates the map used by the identity_map's
      # many_*_many eager loaders to associate the current model object to
      # the associated model objects.  Only used when you use the :eager_graph
      # option a many_*_many association that you are eager loading via +eager+.
      class EagerGraphLoader < Sequel::Model::Associations::EagerGraphLoader
        # Save the key alias and hash used to map associated records to primary records
        # in the custom identity_map eager_loaders.
        def initialize(dataset)
          super
          @key_alias, @map = dataset.opts[:eager_graph_map]
          # Don't include the key aliases in the primary model data
          Array(@key_alias).each{|k| @master_column_map.delete(k)}
        end

        # Create associated model objects without the key aliases used to map them to the current objects,
        # updating the hash used by the custom identity_map eager loader with the values of the key aliases.
        def load(hashes)
          master = master()
          rp = row_procs[master]
          rm = records_map[master]
          dm = dependency_map
          records = []

          key_alias = @key_alias
          uses_lcks = key_alias.is_a?(Array)
          hashes.each do |h|
            # Delete the key aliases from the hash before creating the primary model
            key_alias_value = if uses_lcks
              key_alias.map{|k| h.delete(k)}
            else
              h.delete(key_alias)
            end

            unless key = master_pk(h)
              key = hkey(master_hfor(h))
            end
            unless primary_record = rm[key]
              primary_record = rm[key] = rp.call(master_hfor(h))
              records.push(primary_record)
            end
            # Update the map with the key alias value
            (@map[primary_record.object_id] ||= []) << key_alias_value

            _load(dm, primary_record, h)
          end
      
          unique(records, dm) if @unique
          records
        end
      end
    end
  end
end
