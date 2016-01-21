# frozen-string-literal: true

module Sequel
  module Plugins
    # DatasetAssociations allows you to easily use your model associations
    # via datasets.  For each association you define, it creates a dataset
    # method for that association that returns a dataset of all objects
    # that are associated to objects in the current dataset.  Here's a simple
    # example:
    #
    #   class Artist < Sequel::Model
    #     plugin :dataset_associations
    #     one_to_many :albums
    #   end
    #   Artist.filter(id=>1..100).albums
    #   # SELECT * FROM albums
    #   # WHERE (albums.artist_id IN (
    #   #   SELECT id FROM artists
    #   #   WHERE ((id >= 1) AND (id <= 100))))
    # 
    # This works for all of the association types that ship with Sequel,
    # including ones implemented in other plugins.  Most association options that
    # are supported when eager loading are supported when using a
    # dataset association. However, it will only work for limited associations or
    # *_one associations with orders if the database supports window functions.
    #
    # As the dataset methods return datasets, you can easily chain the
    # methods to get associated datasets of associated datasets:
    #
    #   Artist.filter(id=>1..100).albums.filter{name < 'M'}.tags
    #   # SELECT tags.* FROM tags
    #   # WHERE (tags.id IN (
    #   #   SELECT albums_tags.tag_id FROM albums
    #   #   INNER JOIN albums_tags
    #   #     ON (albums_tags.album_id = albums.id)
    #   #   WHERE
    #   #     ((albums.artist_id IN (
    #   #       SELECT id FROM artists
    #   #       WHERE ((id >= 1) AND (id <= 100)))
    #   #     AND
    #   #     (name < 'M')))))
    # 
    # Usage:
    #
    #   # Make all model subclasses create association methods for datasets
    #   Sequel::Model.plugin :dataset_associations
    #
    #   # Make the Album class create association methods for datasets
    #   Album.plugin :dataset_associations
    module DatasetAssociations
      module ClassMethods
        # Set up a dataset method for each association to return an associated dataset
        def associate(type, name, *)
          ret = super
          r = association_reflection(name)
          meth = r.returns_array? ? name : pluralize(name).to_sym
          def_dataset_method(meth){associated(name)}
          ret
        end

        Plugins.def_dataset_methods(self, :associated)
      end

      module DatasetMethods
        # For the association given by +name+, return a dataset of associated objects
        # such that it would return the union of calling the association method on
        # all objects returned by the current dataset.
        #
        # This supports most options that are supported when eager loading.  However, it
        # will only work for limited associations or *_one associations with orders if the
        # database supports window functions.
        def associated(name)
          raise Error, "unrecognized association name: #{name.inspect}" unless r = model.association_reflection(name)
          ds = r.associated_class.dataset
          sds = opts[:limit] ? self : unordered
          ds = case r[:type]
          when :many_to_one
            ds.filter(r.qualified_primary_key=>sds.select(*Array(r[:qualified_key])))
          when :one_to_one, :one_to_many
            r.send(:apply_filter_by_associations_limit_strategy, ds.filter(r.qualified_key=>sds.select(*Array(r.qualified_primary_key))))
          when :many_to_many, :one_through_one
            mds = r.associated_class.dataset.
              join(r[:join_table], r[:right_keys].zip(r.right_primary_keys)).
              select(*Array(r.qualified_right_key)).
              where(r.qualify(r.join_table_alias, r[:left_keys])=>sds.select(*r.qualify(model.table_name, r[:left_primary_key_columns])))
            ds.filter(r.qualified_right_primary_key=>r.send(:apply_filter_by_associations_limit_strategy, mds))
          when :many_through_many, :one_through_many
            fre = r.reverse_edges.first
            fe, *edges = r.edges
            edges << r.final_edge
            mds = model.
              select(*Array(r.qualify(fre[:table], fre[:left]))).
              join(fe[:table], Array(fe[:right]).zip(Array(fe[:left])), :implicit_qualifier=>model.table_name).
              where(r.qualify(fe[:table], fe[:right])=>sds.select(*r.qualify(model.table_name, r[:left_primary_key_columns])))
            edges.each{|e| mds = mds.join(e[:table], Array(e[:right]).zip(Array(e[:left])))}
            ds.filter(r.qualified_right_primary_key=>r.send(:apply_filter_by_associations_limit_strategy, mds))
          when :pg_array_to_many
            ds.filter(Sequel.expr(r.primary_key=>sds.select{Sequel.pg_array_op(r.qualify(r[:model].table_name, r[:key])).unnest}))
          when :many_to_pg_array
            ds.filter(Sequel.function(:coalesce, Sequel.pg_array_op(r[:key]).overlaps(sds.select{array_agg(r.qualify(r[:model].table_name, r.primary_key))}), false))
          else
            raise Error, "unrecognized association type for association #{name.inspect}: #{r[:type].inspect}"
          end
          r.apply_eager_dataset_changes(ds).unlimited
        end
      end
    end
  end
end
