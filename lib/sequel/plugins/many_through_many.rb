module Sequel
  module Plugins
    # The many_through_many plugin allow you to create an association to multiple objects using multiple join tables.
    # For example, assume the following associations:
    #
    #    Artist.many_to_many :albums
    #    Album.many_to_many :tags
    #
    # The many_through_many plugin would allow this:
    #
    #    Artist.plugin :many_through_many
    #    Artist.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    #
    # Which will give you the tags for all of the artist's albums.
    #
    # Here are some more examples:
    #
    #   # Same as Artist.many_to_many :albums
    #   Artist.many_through_many :albums, [[:albums_artists, :artist_id, :album_id]]
    #
    #   # All artists that are associated to any album that this artist is associated to
    #   Artist.many_through_many :artists, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_artists, :album_id, :artist_id]]
    #
    #   # All albums by artists that are associated to any album that this artist is associated to
    #   Artist.many_through_many :artist_albums, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], \
    #    [:albums_artists, :album_id, :artist_id], [:artists, :id, :id], [:albums_artists, :artist_id, :album_id]], \
    #    :class=>:Album
    #
    #   # All tracks on albums by this artist
    #   Artist.many_through_many :tracks, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id]], \
    #    :right_primary_key=>:album_id
    #
    # Often you don't want the current object to appear in the array of associated objects.  This is easiest to handle via an :after_load hook:
    # 
    #   Artist.many_through_many :artists, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_artists, :album_id, :artist_id]],
    #     :after_load=>proc{|artist, associated_artists| associated_artists.delete(artist)}
    #
    # You can also handle it by adding a dataset block that excludes the current record (so it won't be retrieved at all), but
    # that won't work when eagerly loading, which is why the :after_load proc is recommended instead.
    #
    # It's also common to not want duplicate records, in which case the :distinct option can be used:
    # 
    #   Artist.many_through_many :artists, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_artists, :album_id, :artist_id]],
    #    :distinct=>true
    module ManyThroughMany
      # The AssociationReflection subclass for many_through_many associations.
      class ManyThroughManyAssociationReflection < Sequel::Model::Associations::ManyToManyAssociationReflection
        Sequel::Model::Associations::ASSOCIATION_TYPES[:many_through_many] = self

        # The default associated key alias(es) to use when eager loading
        # associations via eager.
        def default_associated_key_alias
          self[:uses_left_composite_keys] ? (0...self[:through].first[:left].length).map{|i| :"x_foreign_key_#{i}_x"} : :x_foreign_key_x
        end

        %w'associated_key_table predicate_key edges final_edge final_reverse_edge reverse_edges'.each do |meth|
          class_eval(<<-END, __FILE__, __LINE__+1)
            def #{meth}
              cached_fetch(:#{meth}){calculate_edges[:#{meth}]}
            end
          END
        end

        # Many through many associations don't have a reciprocal
        def reciprocal
          nil
        end

        private

        # Make sure to use unique table aliases when lazy loading or eager loading
        def calculate_reverse_edge_aliases(reverse_edges)
          aliases = [associated_class.table_name]
          reverse_edges.each do |e|
            table_alias = e[:table]
            if aliases.include?(table_alias)
              i = 0
              table_alias = loop do
                ta = :"#{table_alias}_#{i}"
                break ta unless aliases.include?(ta)
                i += 1
              end
            end
            aliases.push(e[:alias] = table_alias)
          end
        end

        # Transform the :through option into a list of edges and reverse edges to use to join tables when loading the association.
        def calculate_edges
          es = [{:left_table=>self[:model].table_name, :left_key=>self[:left_primary_key_column]}]
          self[:through].each do |t|
            es.last.merge!(:right_key=>t[:left], :right_table=>t[:table], :join_type=>t[:join_type]||self[:graph_join_type], :conditions=>(t[:conditions]||[]).to_a, :block=>t[:block])
            es.last[:only_conditions] = t[:only_conditions] if t.include?(:only_conditions)
            es << {:left_table=>t[:table], :left_key=>t[:right]}
          end
          es.last.merge!(:right_key=>right_primary_key, :right_table=>associated_class.table_name)
          edges = es.map do |e| 
            h = {:table=>e[:right_table], :left=>e[:left_key], :right=>e[:right_key], :conditions=>e[:conditions], :join_type=>e[:join_type], :block=>e[:block]}
            h[:only_conditions] = e[:only_conditions] if e.include?(:only_conditions)
            h
          end
          reverse_edges = es.reverse.map{|e| {:table=>e[:left_table], :left=>e[:left_key], :right=>e[:right_key]}}
          reverse_edges.pop
          calculate_reverse_edge_aliases(reverse_edges)
          final_reverse_edge = reverse_edges.pop
          final_reverse_alias = final_reverse_edge[:alias]

          h = {:final_edge=>edges.pop,
               :final_reverse_edge=>final_reverse_edge,
               :edges=>edges,
               :reverse_edges=>reverse_edges,
               :predicate_key=>qualify(final_reverse_alias, edges.first[:right]),
               :associated_key_table=>final_reverse_edge[:alias],
          }
          h.each{|k, v| cached_set(k, v)}
          h
        end
      end

      module ClassMethods
        # Create a many_through_many association.  Arguments:
        # * name - Same as associate, the name of the association.
        # * through - The tables and keys to join between the current table and the associated table.
        #   Must be an array, with elements that are either 3 element arrays, or hashes with keys :table, :left, and :right.
        #   The required entries in the array/hash are:
        #   :table (first array element) :: The name of the table to join.
        #   :left (middle array element) :: The key joining the table to the previous table. Can use an
        #                                   array of symbols for a composite key association.
        #   :right (last array element) :: The key joining the table to the next table. Can use an
        #                                  array of symbols for a composite key association.
        #   If a hash is provided, the following keys are respected when using eager_graph:
        #   :block :: A proc to use as the block argument to join.
        #   :conditions :: Extra conditions to add to the JOIN ON clause.  Must be a hash or array of two pairs.
        #   :join_type :: The join type to use for the join, defaults to :left_outer.
        #   :only_conditions :: Conditions to use for the join instead of the ones specified by the keys.
        # * opts - The options for the associaion.  Takes the same options as many_to_many.
        def many_through_many(name, through, opts={}, &block)
          associate(:many_through_many, name, opts.merge(through.is_a?(Hash) ? through : {:through=>through}), &block)
        end

        private

        # Create the association methods and :eager_loader and :eager_grapher procs.
        def def_many_through_many(opts)
          name = opts[:name]
          model = self
          opts[:read_only] = true
          opts[:after_load].unshift(:array_uniq!) if opts[:uniq]
          opts[:cartesian_product_number] ||= 2
          opts[:through] = opts[:through].map do |e|
            case e
            when Array
              raise(Error, "array elements of the through option/argument for many_through_many associations must have at least three elements") unless e.length == 3
              {:table=>e[0], :left=>e[1], :right=>e[2]}
            when Hash
              raise(Error, "hash elements of the through option/argument for many_through_many associations must contain :table, :left, and :right keys") unless e[:table] && e[:left] && e[:right]
              e
            else
              raise(Error, "the through option/argument for many_through_many associations must be an enumerable of arrays or hashes")
            end
          end

          left_key = opts[:left_key] = opts[:through].first[:left]
          uses_lcks = opts[:uses_left_composite_keys] = left_key.is_a?(Array)
          left_keys = Array(left_key)
          left_pk = (opts[:left_primary_key] ||= self.primary_key)
          opts[:eager_loader_key] = left_pk unless opts.has_key?(:eager_loader_key)
          left_pks = opts[:left_primary_keys] = Array(left_pk)
          lpkc = opts[:left_primary_key_column] ||= left_pk
          lpkcs = opts[:left_primary_key_columns] ||= Array(lpkc)
          opts[:dataset] ||= lambda do
            ds = opts.associated_class
            opts.reverse_edges.each{|t| ds = ds.join(t[:table], Array(t[:left]).zip(Array(t[:right])), :table_alias=>t[:alias], :qualify=>:deep)}
            ft = opts.final_reverse_edge
            ds.join(ft[:table],  Array(ft[:left]).zip(Array(ft[:right])) + opts.predicate_keys.zip(left_pks.map{|k| send(k)}), :table_alias=>ft[:alias], :qualify=>:deep)
          end

          left_key_alias = opts[:left_key_alias] ||= opts.default_associated_key_alias
          opts[:eager_loader] ||= lambda do |eo|
            h = eo[:id_map]
            rows = eo[:rows]
            rows.each{|object| object.associations[name] = []}
            ds = opts.associated_class 
            opts.reverse_edges.each{|t| ds = ds.join(t[:table], Array(t[:left]).zip(Array(t[:right])), :table_alias=>t[:alias], :qualify=>:deep)}
            ft = opts.final_reverse_edge
            ds = ds.join(ft[:table], Array(ft[:left]).zip(Array(ft[:right])) + [[opts.predicate_key, h.keys]], :table_alias=>ft[:alias], :qualify=>:deep)
            ds = model.eager_loading_dataset(opts, ds, nil, eo[:associations], eo)
            case opts.eager_limit_strategy
            when :window_function
              delete_rn = true
              rn = ds.row_number_column
              ds = apply_window_function_eager_limit_strategy(ds, opts)
            when :correlated_subquery
              ds = apply_correlated_subquery_eager_limit_strategy(ds, opts) do |xds|
                dsa = ds.send(:dataset_alias, 2)
                opts.reverse_edges.each{|t| xds = xds.join(t[:table], Array(t[:left]).zip(Array(t[:right])), :table_alias=>t[:alias])}
                xds.join(ft[:table], Array(ft[:left]).zip(Array(ft[:right])) + left_keys.map{|k| [k, SQL::QualifiedIdentifier.new(ft[:table], k)]}, :table_alias=>dsa, :qualify=>:deep)
              end
            end
            ds.all do |assoc_record|
              assoc_record.values.delete(rn) if delete_rn
              hash_key = if uses_lcks
                left_key_alias.map{|k| assoc_record.values.delete(k)}
              else
                assoc_record.values.delete(left_key_alias)
              end
              next unless objects = h[hash_key]
              objects.each{|object| object.associations[name].push(assoc_record)}
            end
            if opts.eager_limit_strategy == :ruby
              limit, offset = opts.limit_and_offset
              rows.each{|o| o.associations[name] = o.associations[name].slice(offset||0, limit) || []}
            end
          end

          join_type = opts[:graph_join_type]
          select = opts[:graph_select]
          graph_block = opts[:graph_block]
          only_conditions = opts[:graph_only_conditions]
          use_only_conditions = opts.include?(:graph_only_conditions)
          conditions = opts[:graph_conditions]
          opts[:eager_grapher] ||= proc do |eo|
            ds = eo[:self]
            iq = eo[:implicit_qualifier]
            opts.edges.each do |t|
              ds = ds.graph(t[:table], t.fetch(:only_conditions, (Array(t[:right]).zip(Array(t[:left])) + t[:conditions])), :select=>false, :table_alias=>ds.unused_table_alias(t[:table]), :join_type=>t[:join_type], :qualify=>:deep, :implicit_qualifier=>iq, &t[:block])
              iq = nil
            end
            fe = opts.final_edge
            ds.graph(opts.associated_class, use_only_conditions ? only_conditions : (Array(opts.right_primary_key).zip(Array(fe[:left])) + conditions), :select=>select, :table_alias=>eo[:table_alias], :qualify=>:deep, :join_type=>join_type, &graph_block)
          end

          def_association_dataset_methods(opts)
        end
      end

      module DatasetMethods
        private

        # Use a subquery to filter rows to those related to the given associated object
        def many_through_many_association_filter_expression(op, ref, obj)
          lpks = ref[:left_primary_key_columns]
          lpks = lpks.first if lpks.length == 1
          lpks = ref.qualify(model.table_name, lpks)
          edges = ref.edges
          first, rest = edges.first, edges[1..-1]
          last = edges.last
          ds = model.db[first[:table]].select(*Array(ref.qualify(first[:table], first[:right])))
          rest.each{|e| ds = ds.join(e[:table], e.fetch(:only_conditions, (Array(e[:right]).zip(Array(e[:left])) + e[:conditions])), :table_alias=>ds.unused_table_alias(e[:table]), :qualify=>:deep, &e[:block])}
          last_alias = if rest.empty?
            first[:table]
          else
            last_join = ds.opts[:join].last
            last_join.table_alias || last_join.table
          end

          meths = if obj.is_a?(Sequel::Dataset)
            ref.qualify(obj.model.table_name, ref.right_primary_keys)
          else
            ref.right_primary_key_methods
          end

          exp = association_filter_key_expression(ref.qualify(last_alias, Array(ref.final_edge[:left])), meths, obj)
          if exp == SQL::Constants::FALSE
            association_filter_handle_inversion(op, exp, Array(lpks))
          else
            ds = ds.where(exp).exclude(SQL::BooleanExpression.from_value_pairs(ds.opts[:select].zip([]), :OR))
            association_filter_handle_inversion(op, SQL::BooleanExpression.from_value_pairs(lpks=>ds), Array(lpks))
          end
        end
      end
    end
  end
end
