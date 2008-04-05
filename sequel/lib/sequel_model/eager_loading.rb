# Eager loading makes it so that you can load all associated records for a
# set of objects in a single query, instead of a separate query for each object.
#
# The basic idea for how it works is that the dataset is first loaded normally.
# Then it goes through all associations that have been specified via .eager.
# It loads each of those associations separately, then associates them back
# to the original dataset via primary/foreign keys.  Due to the necessity of
# all objects being present, you need to use .all to use eager loading, as it
# can't work with .each.
#
# This implementation avoids the complexity of extracting an object graph out
# of a single dataset, by building the object graph out of multiple datasets,
# one for each association.  By using a separate dataset for each association,
# it avoids problems such as aliasing conflicts and creating cartesian product
# result sets if multiple *_to_many eager associations are requested.
#
# One limitation of using this method is that you cannot filter the dataset
# based on values of columns in an associated table, since the associations are loaded
# in separate queries.  To do that you need to load all associations in the
# same query, and extract an object graph from the results of that query.
#
# You can cascade the eager loading (loading associations' associations)
# with no limit to the depth of the cascades.  You do this by passing a hash to .eager
# with the keys being associations of the current model and values being
# associations of the model associated with the current model via the key.
#
# The associations' order, if defined, is respected.  You cannot eagerly load
# an association with a block argument, as the block argument is evaluated in
# terms of a specific instance of the model, and no specific instance exists
# when eagerly loading.
module Sequel::Model::Associations::EagerLoading
  # Add associations to the list of associations to eagerly load.
  # Associations can be a symbol or a hash with symbol keys (for cascaded
  # eager loading). Examples:
  #
  #  Album.eager(:artist).all
  #  Album.eager(:artist, :genre).all
  #  Album.eager(:artist).eager(:genre).all
  #  Artist.eager(:albums=>:tracks).all
  #  Artist.eager(:albums=>{:tracks=>:genre}).all
  def eager(*associations)
    raise(ArgumentError, 'No model for this dataset') unless @opts[:models] && model = @opts[:models][nil]
    opt = @opts[:eager]
    opt = opt ? opt.dup : {}
    check = Proc.new do |a|
      raise(ArgumentError, 'Invalid association') unless reflection = model.association_reflection(a)
      raise(ArgumentError, 'Cannot eagerly load associations with block arguments') if reflection[:block]
    end
    associations.flatten.each do |association|
      case association
        when Symbol
          check.call(association)
          opt[association] = nil
        when Hash
          association.keys.each{|assoc| check.call(assoc)}
          opt.merge!(association)
        else raise(ArgumentError, 'Associations must be in the form of a symbol or hash')
      end
    end
    ds = clone(:eager=>opt)
    ds.add_callback(:post_load, :eager_load) unless @opts[:eager] 
    ds
  end
  
  private
    # Eagerly load all specified associations 
    def eager_load(a)
      return if a.empty?
      # Current model class
      model = @opts[:models][nil]
      # All associations to eager load
      eager_assoc = @opts[:eager]
      # Key is foreign/primary key name symbol
      # Value is hash with keys being foreign/primary key values (generally integers)
      #  and values being an array of current model objects with that
      #  specific foreign/primary key
      key_hash = {}
      # array of attribute_values keys to monitor
      keys = []
      # Reflections for all associations to eager load
      reflections = eager_assoc.keys.collect{|assoc| model.association_reflection(assoc)}

      # Populate keys to monitor
      reflections.each do |reflection|
        key = reflection[:type] == :many_to_one ? reflection[:key] : model.primary_key
        next if key_hash[key]
        key_hash[key] = {}
        keys << key
      end
      
      # Associate each object with every key being monitored
      a.each do |r|
        keys.each do |key|
          ((key_hash[key][r[key]] ||= []) << r) if r[key]
        end
      end
      
      # Iterate through eager associations and assign instance variables
      # for the association for all model objects
      reflections.each do |reflection|
        assoc_class = model.send(:associated_class, reflection)
        assoc_name = reflection[:name]
        # Proc for setting cascaded eager loading
        cascade = Proc.new do |d|
          if c = eager_assoc[assoc_name]
            d = d.eager(c)
          end
          if c = reflection[:eager]
            d = d.eager(c)
          end
          d
        end
        case rtype = reflection[:type]
          when :many_to_one
            key = reflection[:key]
            h = key_hash[key]
            keys = h.keys
            # No records have the foreign key set for this association, so skip it
            next unless keys.length > 0
            ds = assoc_class.filter(assoc_class.primary_key=>keys)
            ds = cascade.call(ds)
            ds.all do |assoc_object|
              h[assoc_object.pk].each do |object|
                object.instance_variable_set(:"@#{assoc_name}", assoc_object)
              end
            end
          when :one_to_many, :many_to_many
            if rtype == :one_to_many
              fkey = key = reflection[:key]
              h = key_hash[model.primary_key]
              reciprocal = model.send(:reciprocal_association, reflection)
              ds = assoc_class.filter(key=>h.keys)
            else
              assoc_table = assoc_class.table_name
              left = reflection[:left_key]
              right = reflection[:right_key]
              right_pk = (reflection[:right_primary_key] || :"#{assoc_table}__#{assoc_class.primary_key}")
              join_table = reflection[:join_table]
              fkey = (opts[:left_key_alias] ||= :"x_foreign_key_x")
              table_selection = (opts[:select] ||= assoc_table.all)
              key_selection = (opts[:left_key_select] ||= :"#{join_table}__#{left}___#{fkey}")
              h = key_hash[model.primary_key]
              ds = assoc_class.select(table_selection, key_selection).inner_join(join_table, right=>right_pk, left=>h.keys)
            end
            if order = reflection[:order]
              ds = ds.order(order)
            end
            ds = cascade.call(ds)
            ivar = :"@#{assoc_name}"
            h.values.each do |object_array|
              object_array.each do |object|
                object.instance_variable_set(ivar, [])
              end
            end
            ds.all do |assoc_object|
              fk = if rtype == :many_to_many
                assoc_object.values.delete(fkey)
              else
                assoc_object[fkey]
              end
              h[fk].each do |object|
                object.instance_variable_get(ivar) << assoc_object
                assoc_object.instance_variable_set(reciprocal, object) if reciprocal
              end
            end
        end
      end
    end
end
