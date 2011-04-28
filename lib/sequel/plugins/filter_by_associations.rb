module Sequel
  module Plugins
    # The filter_by_associations plugin allows filtering by associations defined
    # in the model.  To use this, you should use the association name as a hash key
    # and the associated model object as a hash value:
    # 
    #   Album.many_to_one :artist
    #   Album.filter(:artist=>Artist[1])
    # 
    # This doesn't just work for +many_to_one+ associations, it also works for
    # +one_to_one+, +one_to_many+, and +many_to_many+ associations:
    #
    #   Album.one_to_one :album_info
    #   Album.filter(:album_info=>AlbumInfo[2])
    #
    #   Album.one_to_many :tracks
    #   Album.filter(:tracks=>Track[3])
    #
    #   Album.many_to_many :tags
    #   Album.filter(:tags=>Tag[4])
    #
    # Note that for +one_to_many+ and +many_to_many+ associations, you still
    # use the plural form even though only a single model object is given.
    # You cannot use an array of model objects as the value, only a single model
    # object.  To use separate model objects for the same association, you can
    # use the array form of condition specifiers:
    #
    #   Album.filter([[:tags, Tag[1]], [:tags, Tag[2]]])
    #
    # That will return albums associated with both tag 1 and tag 2.
    #
    # Usage:
    #
    #   # Enable filtering by associations for all model datasets
    #   Sequel::Model.plugin :filter_by_associations
    #
    #   # Enable filtering by associations for just the Album dataset
    #   Album.plugin :filter_by_associations
    module FilterByAssociations
      module DatasetMethods
        # If the expression is in the form <tt>x = y</tt> where +y+ is a <tt>Sequel::Model</tt>
        # instance, assume +x+ is an association symbol and look up the association reflection
        # via the dataset's model.  From there, return the appropriate SQL based on the type of
        # association and the values of the foreign/primary keys of +y+.  For most association
        # types, this is a simple transformation, but for +many_to_many+ associations this 
        # creates a subquery to the join table.
        def complex_expression_sql(op, args)
          if op == :'=' and args.at(1).is_a?(Sequel::Model)
            l, r = args
            if a = model.association_reflections[l]
              unless r.is_a?(a.associated_class)
                raise Sequel::Error, "invalid association class #{r.class.inspect} for association #{l.inspect} used in dataset filter for model #{model.inspect}, expected class #{a.associated_class.inspect}"
              end

              case a[:type]
              when :many_to_one
                literal(SQL::BooleanExpression.from_value_pairs(a[:keys].zip(a.primary_keys.map{|k| r.send(k)})))
              when :one_to_one, :one_to_many
                literal(SQL::BooleanExpression.from_value_pairs(a[:primary_keys].zip(a[:keys].map{|k| r.send(k)})))
              when :many_to_many
                lpks, lks, rks = a.values_at(:left_primary_keys, :left_keys, :right_keys)
                lpks = lpks.first if lpks.length == 1
                literal(SQL::BooleanExpression.from_value_pairs(lpks=>model.db[a[:join_table]].select(*lks).where(rks.zip(a.right_primary_keys.map{|k| r.send(k)}))))
              else
                raise Sequel::Error, "invalid association type #{a[:type].inspect} for association #{l.inspect} used in dataset filter for model #{model.inspect}"
              end
            else
              raise Sequel::Error, "invalid association #{l.inspect} used in dataset filter for model #{model.inspect}"
            end
          else
            super
          end
        end
      end
    end
  end
end
