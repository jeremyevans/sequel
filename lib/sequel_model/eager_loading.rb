# Eager loading makes it so that you can load all associated records for a
# set of objects in a single query, instead of a separate query for each object.
#
# Two separate implementations are provided.  #eager should be used most of the
# time, as it loads associated records using one query per association.  However,
# it does not allow you the ability to filter based on columns in associated tables.  #eager_graph loads
# all records in one query.  Using #eager_graph you can filter based on columns in associated
# tables.  However, #eager_graph can be much slower than #eager, especially if multiple
# *_to_many associations are joined.
#
# You can cascade the eager loading (loading associations' associations)
# with no limit to the depth of the cascades.  You do this by passing a hash to #eager or #eager_graph
# with the keys being associations of the current model and values being
# associations of the model associated with the current model via the key.
#  
# The arguments can be symbols or hashes with symbol keys (for cascaded
# eager loading). Examples:
#
#   Album.eager(:artist).all
#   Album.eager_graph(:artist).all
#   Album.eager(:artist, :genre).all
#   Album.eager_graph(:artist, :genre).all
#   Album.eager(:artist).eager(:genre).all
#   Album.eager_graph(:artist).eager(:genre).all
#   Artist.eager(:albums=>:tracks).all
#   Artist.eager_graph(:albums=>:tracks).all
#   Artist.eager(:albums=>{:tracks=>:genre}).all
#   Artist.eager_graph(:albums=>{:tracks=>:genre}).all
module Sequel::Model::Associations::EagerLoading
  # Add the #eager! and #eager_graph! mutation methods to the dataset.
  def self.extended(obj)
    obj.def_mutation_method(:eager, :eager_graph)
  end

  # The preferred eager loading method.  Loads all associated records using one
  # query for each association.
  #
  # The basic idea for how it works is that the dataset is first loaded normally.
  # Then it goes through all associations that have been specified via eager.
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
  # same query, and extract an object graph from the results of that query. If you
  # need to filter based on columns in associated tables, look at #eager_graph
  # or join the tables you need to filter on manually. 
  #
  # Each association's order, if defined, is respected. Eager also works
  # on a limited dataset, but does not use any :limit options for associations.
  # If the association uses a block or has an :eager_block argument, it is used.
  def eager(*associations)
    opt = @opts[:eager]
    opt = opt ? opt.dup : {}
    associations.flatten.each do |association|
      case association
        when Symbol
          check_association(model, association)
          opt[association] = nil
        when Hash
          association.keys.each{|assoc| check_association(model, assoc)}
          opt.merge!(association)
        else raise(Sequel::Error, 'Associations must be in the form of a symbol or hash')
      end
    end
    clone(:eager=>opt)
  end

  # The secondary eager loading method.  Loads all associations in a single query. This
  # method should only be used if you need to filter based on columns in associated tables.
  #
  # This method builds an object graph using Dataset#graph.  Then it uses the graph
  # to build the associations, and finally replaces the graph with a simple array
  # of model objects.
  #
  # Be very careful when using this with multiple *_to_many associations, as you can
  # create large cartesian products.  If you must graph multiple *_to_many associations,
  # make sure your filters are specific if you have a large database.
  # 
  # Each association's order, if definied, is respected. #eager_graph probably
  # won't work correctly on a limited dataset, unless you are
  # only graphing many_to_one associations.
  # 
  # Does not use the block defined for the association, since it does a single query for
  # all objects.  You can use the :graph_join_type, :graph_conditions, and :graph_join_table_conditions
  # association options to modify the SQL query.
  def eager_graph(*associations)
    table_name = model.table_name
    ds = if @opts[:eager_graph]
      self
    else
      # Each of the following have a symbol key for the table alias, with the following values: 
      # :reciprocals - the reciprocal instance variable to use for this association
      # :requirements - array of requirements for this association
      # :alias_association_type_map - the type of association for this association
      # :alias_association_name_map - the name of the association for this association
      clone(:eager_graph=>{:requirements=>{}, :master=>model.table_name, :alias_association_type_map=>{}, :alias_association_name_map=>{}, :reciprocals=>{}})
    end
    ds.eager_graph_associations(ds, model, table_name, [], *associations)
  end
  
  protected

  # Call graph on the association with the correct arguments,
  # update the eager_graph data structure, and recurse into
  # eager_graph_associations if there are any passed in associations
  # (which would be dependencies of the current association)
  #
  # Arguments:
  # * ds - Current dataset
  # * model - Current Model
  # * ta - table_alias used for the parent association
  # * requirements - an array, used as a stack for requirements
  # * r - association reflection for the current association
  # * *associations - any associations dependent on this one
  def eager_graph_association(ds, model, ta, requirements, r, *associations)
    klass = r.associated_class
    assoc_name = r[:name]
    assoc_table_alias = ds.eager_unique_table_alias(ds, assoc_name)
    ds = r[:eager_grapher].call(ds, assoc_table_alias, ta)
    ds = ds.order_more(*Array(r[:order]).map{|c| eager_graph_qualify_order(assoc_table_alias, c)}) if r[:order] and r[:order_eager_graph]
    eager_graph = ds.opts[:eager_graph]
    eager_graph[:requirements][assoc_table_alias] = requirements.dup
    eager_graph[:alias_association_name_map][assoc_table_alias] = assoc_name
    eager_graph[:alias_association_type_map][assoc_table_alias] = r.returns_array?
    ds = ds.eager_graph_associations(ds, r.associated_class, assoc_table_alias, requirements + [assoc_table_alias], *associations) unless associations.empty?
    ds
  end

  # Check the associations are valid for the given model.
  # Call eager_graph_association on each association.
  #
  # Arguments:
  # * ds - Current dataset
  # * model - Current Model
  # * ta - table_alias used for the parent association
  # * requirements - an array, used as a stack for requirements
  # * *associations - the associations to add to the graph
  def eager_graph_associations(ds, model, ta, requirements, *associations)
    return ds if associations.empty?
    associations.flatten.each do |association|
      ds = case association
      when Symbol
        ds.eager_graph_association(ds, model, ta, requirements, check_association(model, association))
      when Hash
        association.each do |assoc, assoc_assocs|
          ds = ds.eager_graph_association(ds, model, ta, requirements, check_association(model, assoc), assoc_assocs)
        end
        ds
      else raise(Sequel::Error, 'Associations must be in the form of a symbol or hash')
      end
    end
    ds
  end

  # Build associations out of the array of returned object graphs.
  def eager_graph_build_associations(record_graphs)
    eager_graph = @opts[:eager_graph]
    master = eager_graph[:master]
    requirements = eager_graph[:requirements]
    alias_map = eager_graph[:alias_association_name_map]
    type_map = eager_graph[:alias_association_type_map]
    reciprocal_map = eager_graph[:reciprocals]

    # Make dependency map hash out of requirements array for each association.
    # This builds a tree of dependencies that will be used for recursion
    # to ensure that all parts of the object graph are loaded into the
    # appropriate subordinate association.
    dependency_map = {}
    # Sort the associations be requirements length, so that
    # requirements are added to the dependency hash before their
    # dependencies.
    requirements.sort_by{|a| a[1].length}.each do |ta, deps|
      if deps.empty?
        dependency_map[ta] = {}
      else
        deps = deps.dup
        hash = dependency_map[deps.shift]
        deps.each do |dep|
          hash = hash[dep]
        end
        hash[ta] = {}
      end
    end

    # This mapping is used to make sure that duplicate entries in the
    # result set are mapped to a single record.  For example, using a
    # single one_to_many association with 10 associated records,
    # the main object will appear in the object graph 10 times.
    # We map by primary key, if available, or by the object's entire values,
    # if not. The mapping must be per table, so create sub maps for each table
    # alias.
    records_map = {master=>{}}
    alias_map.keys.each{|ta| records_map[ta] = {}}

    # This will hold the final record set that we will be replacing the object graph with.
    records = []
    record_graphs.each do |record_graph|
      primary_record = record_graph[master]
      key = primary_record.pk || primary_record.values.sort_by{|x| x[0].to_s}
      if cached_pr = records_map[master][key]
        primary_record = cached_pr
      else
        records_map[master][key] = primary_record
        # Only add it to the list of records to return if it is a new record
        records.push(primary_record)
      end
      # Build all associations for the current object and it's dependencies
      eager_graph_build_associations_graph(dependency_map, alias_map, type_map, reciprocal_map, records_map, primary_record, record_graph)
    end

    # Remove duplicate records from all associations if this graph could possibly be a cartesian product
    eager_graph_make_associations_unique(records, dependency_map, alias_map, type_map) if type_map.values.select{|v| v}.length > 1
    
    # Replace the array of object graphs with an array of model objects
    record_graphs.replace(records)
  end

  # Creates a unique table alias that hasn't already been used in the query.
  # Will either be the table_alias itself or table_alias_N for some integer
  # N (starting at 0 and increasing until an unused one is found).
  def eager_unique_table_alias(ds, table_alias)
    used_aliases = ds.opts[:from]
    graph = ds.opts[:graph]
    used_aliases += graph[:table_aliases].keys if graph
    if used_aliases.include?(table_alias)
      i = 0
      loop do
        ta = :"#{table_alias}_#{i}"
        return ta unless used_aliases.include?(ta)
        i += 1
      end
    end
    table_alias
  end

  private

  # Make sure the association is valid for this model, and return the related AssociationReflection.
  def check_association(model, association)
    raise(Sequel::Error, 'Invalid association') unless reflection = model.association_reflection(association)
    raise(Sequel::Error, "Eager loading is not allowed for #{model.name} association #{association}") if reflection[:allow_eager] == false
    reflection
  end

  # Build associations for the current object.  This is called recursively
  # to build object's dependencies.
  def eager_graph_build_associations_graph(dependency_map, alias_map, type_map, reciprocal_map, records_map, current, record_graph)
    return if dependency_map.empty?
    # Don't clobber the instance variable array for *_to_many associations if it has already been setup
    dependency_map.keys.each do |ta|
      assoc_name = alias_map[ta]
      current.associations[assoc_name] = type_map[ta] ? [] : nil unless current.associations.include?(assoc_name)
    end
    dependency_map.each do |ta, deps|
      next unless rec = record_graph[ta]
      key = rec.pk || rec.values.sort_by{|x| x[0].to_s}
      if cached_rec = records_map[ta][key]
        rec = cached_rec
      else
        records_map[ta][rec.pk] = rec
      end
      assoc_name = alias_map[ta]
      case type_map[ta]
      when false
        current.associations[assoc_name] = rec
      else
        current.associations[assoc_name].push(rec) 
        if reciprocal = reciprocal_map[ta]
          rec.associations[reciprocal] = current
        end
      end
      # Recurse into dependencies of the current object
      eager_graph_build_associations_graph(deps, alias_map, type_map, reciprocal_map, records_map, rec, record_graph)
    end
  end

  # If the result set is the result of a cartesian product, then it is possible that
  # there are multiple records for each association when there should only be one.
  # In that case, for each object in all associations loaded via #eager_graph, run
  # uniq! on the association to make sure no duplicate records show up.
  # Note that this can cause legitimate duplicate records to be removed.
  def eager_graph_make_associations_unique(records, dependency_map, alias_map, type_map)
    records.each do |record|
      dependency_map.each do |ta, deps|
        list = if !type_map[ta]
          item = record.send(alias_map[ta])
          [item] if item
        else
          list = record.send(alias_map[ta])
          list.uniq!
        end
        # Recurse into dependencies
        eager_graph_make_associations_unique(list, deps, alias_map, type_map) if list
      end
    end
  end

  # Qualify the given expression if necessary.  The only expressions which are qualified are
  # unqualified symbols and identifiers, either of which may by sorted.
  def eager_graph_qualify_order(table_alias, expression)
    case expression
    when Symbol
      table, column, aliaz = split_symbol(expression)
      raise(Sequel::Error, "Can't use an aliased expression in the :order option") if aliaz
      table ? expression : Sequel::SQL::QualifiedIdentifier.new(table_alias, expression)
    when Sequel::SQL::Identifier
      Sequel::SQL::QualifiedIdentifier.new(table_alias, expression)
    when Sequel::SQL::OrderedExpression
      Sequel::SQL::OrderedExpression.new(eager_graph_qualify_order(table_alias, expression.expression), expression.descending)
    else
      expression
    end
  end

  # Eagerly load all specified associations 
  def eager_load(a)
    return if a.empty?
    # All associations to eager load
    eager_assoc = @opts[:eager]
    # Key is foreign/primary key name symbol
    # Value is hash with keys being foreign/primary key values (generally integers)
    #  and values being an array of current model objects with that
    #  specific foreign/primary key
    key_hash = {}
    # Reflections for all associations to eager load
    reflections = eager_assoc.keys.collect{|assoc| model.association_reflection(assoc)}

    # Populate keys to monitor
    reflections.each{|reflection| key_hash[reflection.eager_loader_key] ||= Hash.new{|h,k| h[k] = []}}
    
    # Associate each object with every key being monitored
    a.each do |rec|
      key_hash.each do |key, id_map|
        id_map[rec[key]] << rec if rec[key]
      end
    end
    
    reflections.each do |r|
      r[:eager_loader].call(key_hash, a, eager_assoc[r[:name]])
      a.each{|object| object.send(:run_association_callbacks, r, :after_load, object.associations[r[:name]])} unless r[:after_load].empty?
    end 
  end

  # Build associations from the graph if #eager_graph was used, 
  # and/or load other associations if #eager was used.
  def post_load(all_records)
    eager_graph_build_associations(all_records) if @opts[:eager_graph]
    eager_load(all_records) if @opts[:eager]
  end
end
