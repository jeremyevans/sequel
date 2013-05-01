module Sequel
  module Plugins
    # = Overview
    #
    # The rcte_tree plugin deals with tree structured data stored
    # in the database using the adjacency list model (where child rows
    # have a foreign key pointing to the parent rows), using recursive
    # common table expressions to load all ancestors in a single query,
    # all descendants in a single query, and all descendants to a given
    # level (where level 1 is children, level 2 is children and grandchildren
    # etc.) in a single query.
    #
    # = Background
    # 
    # There are two types of common models for storing tree structured data
    # in an SQL database, the adjacency list model and the nested set model.
    # Before recursive common table expressions (or similar capabilities such
    # as CONNECT BY for Oracle), the nested set model was the only easy way
    # to retrieve all ancestors and descendants in a single query.  However,
    # it has significant performance corner cases.
    #
    # On PostgreSQL 8.4, with a significant number of rows, the nested set
    # model is almost 500 times slower than using a recursive common table
    # expression with the adjacency list model to get all descendants, and
    # almost 24,000 times slower to get all descendants to a given level.
    #
    # Considering that the nested set model requires more difficult management
    # than the adjacency list model, it's almost always better to use the
    # adjacency list model if your database supports common table expressions.
    # See http://explainextended.com/2009/09/24/adjacency-list-vs-nested-sets-postgresql/
    # for detailed analysis.
    #
    # = Usage
    #
    # The rcte_tree plugin adds four associations to the model: parent, children, ancestors, and
    # descendants.  Both the parent and children are fairly standard many_to_one
    # and one_to_many associations, respectively.  However, the ancestors and
    # descendants associations are special.  Both the ancestors and descendants
    # associations will automatically set the parent and children associations,
    # respectively, for current object and all of the ancestor or descendant
    # objects, whenever they are loaded (either eagerly or lazily).  Additionally,
    # the descendants association can take a level argument when called eagerly,
    # which limits the returned objects to only that many levels in the tree (see
    # the Overview).
    #
    #   Model.plugin :rcte_tree
    #   
    #   # Lazy loading
    #   model = Model.first
    #   model.parent
    #   model.children
    #   model.ancestors # Populates :parent association for all ancestors
    #   model.descendants # Populates :children association for all descendants
    #   
    #   # Eager loading - also populates the :parent and children associations
    #   # for all ancestors and descendants
    #   Model.filter(:id=>[1, 2]).eager(:ancestors, :descendants).all
    #   
    #   # Eager loading children and grand children
    #   Model.filter(:id=>[1, 2]).eager(:descendants=>2).all
    #   # Eager loading children, grand children, and great grand children
    #   Model.filter(:id=>[1, 2]).eager(:descendants=>3).all
    #
    # = Options
    #
    # You can override the options for any specific association by making
    # sure the plugin options contain one of the following keys:
    #
    # * :parent - hash of options for the parent association
    # * :children - hash of options for the children association
    # * :ancestors - hash of options for the ancestors association
    # * :descendants - hash of options for the descendants association
    #
    # Note that you can change the name of the above associations by specifying
    # a :name key in the appropriate hash of options above.  For example:
    #
    #   Model.plugin :rcte_tree, :parent=>{:name=>:mother},
    #    :children=>{:name=>:daughters}, :descendants=>{:name=>:offspring}
    #
    # Any other keys in the main options hash are treated as options shared by
    # all of the associations.  Here's a few options that affect the plugin:
    #
    # * :key - The foreign key in the table that points to the primary key
    #   of the parent (default: :parent_id)
    # * :primary_key - The primary key to use (default: the model's primary key)
    # * :key_alias - The symbol identifier to use for aliasing when eager
    #   loading (default: :x_root_x)
    # * :cte_name - The symbol identifier to use for the common table expression
    #   (default: :t)
    # * :level_alias - The symbol identifier to use when eagerly loading descendants
    #   up to a given level (default: :x_level_x)
    module RcteTree
      # Create the appropriate parent, children, ancestors, and descendants
      # associations for the model.
      def self.apply(model, opts={})
        model.plugin :tree, opts

        opts = opts.dup
        opts[:class] = model
        opts[:methods_module] = Module.new
        model.send(:include, opts[:methods_module])
        
        key = opts[:key] ||= :parent_id
        prkey = opts[:primary_key] ||= model.primary_key
        
        parent = opts.merge(opts.fetch(:parent, {})).fetch(:name, :parent)
        childrena = opts.merge(opts.fetch(:children, {})).fetch(:name, :children)
        
        ka = opts[:key_alias] ||= :x_root_x
        t = opts[:cte_name] ||= :t
        opts[:reciprocal] = nil
        c_all = if model.dataset.recursive_cte_requires_column_aliases?
          # Work around Oracle/ruby-oci8 bug that returns integers as BigDecimals in recursive queries.
          conv_bd = model.db.database_type == :oracle
          col_aliases = model.dataset.columns
          model_table = model.table_name
          col_aliases.map{|c| SQL::QualifiedIdentifier.new(model_table, c)}
        else
          [SQL::ColumnAll.new(model.table_name)]
        end
        
        a = opts.merge(opts.fetch(:ancestors, {}))
        ancestors = a.fetch(:name, :ancestors)
        a[:read_only] = true unless a.has_key?(:read_only)
        a[:eager_loader_key] = key
        a[:dataset] ||= proc do
          base_ds = model.filter(prkey=>send(key))
          recursive_ds = model.join(t, key=>prkey)
          if c = a[:conditions]
            (base_ds, recursive_ds) = [base_ds, recursive_ds].collect do |ds|
              (c.is_a?(Array) && !Sequel.condition_specifier?(c)) ? ds.filter(*c) : ds.filter(c)
            end
          end
          table_alias = model.dataset.schema_and_table(model.table_name)[1].to_sym
          model.from(SQL::AliasedExpression.new(t, table_alias)).
           with_recursive(t, col_aliases ? base_ds.select(*col_aliases) : base_ds.select_all,
            recursive_ds.select(*c_all),
            :args=>col_aliases)
        end
        aal = Array(a[:after_load])
        aal << proc do |m, ancs|
          unless m.associations.has_key?(parent)
            parent_map = {m[prkey]=>m}
            child_map = {}
            child_map[m[key]] = m if m[key]
            m.associations[parent] = nil
            ancs.each do |obj|
              obj.associations[parent] = nil
              parent_map[obj[prkey]] = obj
              if ok = obj[key]
                child_map[ok] = obj
              end
            end
            parent_map.each do |parent_id, obj|
              if child = child_map[parent_id]
                child.associations[parent] = obj
              end
            end
          end
        end
        a[:after_load] ||= aal
        a[:eager_loader] ||= proc do |eo|
          id_map = eo[:id_map]
          parent_map = {}
          children_map = {}
          eo[:rows].each do |obj|
            parent_map[obj[prkey]] = obj
            (children_map[obj[key]] ||= []) << obj
            obj.associations[ancestors] = []
            obj.associations[parent] = nil
          end
          r = model.association_reflection(ancestors)
          base_case = model.filter(prkey=>id_map.keys).
           select(SQL::AliasedExpression.new(prkey, ka), *c_all)
          recursive_case = model.join(t, key=>prkey).
           select(SQL::QualifiedIdentifier.new(t, ka), *c_all)
          if c = r[:conditions]
            (base_case, recursive_case) = [base_case, recursive_case].collect do |ds|
              (c.is_a?(Array) && !Sequel.condition_specifier?(c)) ? ds.filter(*c) : ds.filter(c)
            end
          end
          table_alias = model.dataset.schema_and_table(model.table_name)[1].to_sym
          elds = model.eager_loading_dataset(r,
           model.from(SQL::AliasedExpression.new(t, table_alias)).
            with_recursive(t, base_case,
             recursive_case,
             :args=>(([ka] + col_aliases) if col_aliases)),
           r.select,
           eo[:associations], eo)
          elds = elds.select_append(ka) unless elds.opts[:select] == nil
          elds.all do |obj|
            opk = obj[prkey]
            if parent_map.has_key?(opk)
              if idm_obj = parent_map[opk]
                idm_obj.values[ka] = obj.values[ka]
                obj = idm_obj
              end
            else
              obj.associations[parent] = nil
              parent_map[opk] = obj
              (children_map[obj[key]] ||= []) << obj
            end
            
            kv = obj.values.delete(ka)
            kv = kv.to_i if conv_bd && kv.is_a?(BigDecimal)
            if roots = id_map[kv]
              roots.each do |root|
                root.associations[ancestors] << obj
              end
            end
          end
          parent_map.each do |parent_id, obj|
            if children = children_map[parent_id]
              children.each do |child|
                child.associations[parent] = obj
              end
            end
          end
        end
        model.one_to_many ancestors, a
        
        d = opts.merge(opts.fetch(:descendants, {}))
        descendants = d.fetch(:name, :descendants)
        d[:read_only] = true unless d.has_key?(:read_only)
        la = d[:level_alias] ||= :x_level_x
        d[:dataset] ||= proc do
          base_ds = model.filter(key=>send(prkey))
          recursive_ds = model.join(t, prkey=>key)
          if c = d[:conditions]
            (base_ds, recursive_ds) = [base_ds, recursive_ds].collect do |ds|
              (c.is_a?(Array) && !Sequel.condition_specifier?(c)) ? ds.filter(*c) : ds.filter(c)
            end
          end
          table_alias = model.dataset.schema_and_table(model.table_name)[1].to_sym
          model.from(SQL::AliasedExpression.new(t, table_alias)).
           with_recursive(t, col_aliases ? base_ds.select(*col_aliases) : base_ds.select_all,
            recursive_ds.select(*c_all),
            :args=>col_aliases)
          end
        dal = Array(d[:after_load])
        dal << proc do |m, descs|
          unless m.associations.has_key?(childrena)
            parent_map = {m[prkey]=>m}
            children_map = {}
            m.associations[childrena] = []
            descs.each do |obj|
              obj.associations[childrena] = []
              if opk = obj[prkey]
                parent_map[opk] = obj
              end
              if ok = obj[key]
                (children_map[ok] ||= []) << obj
              end
            end
            children_map.each do |parent_id, objs|
              parent_map[parent_id].associations[childrena] = objs
            end
          end
        end
        d[:after_load] = dal
        d[:eager_loader] ||= proc do |eo|
          id_map = eo[:id_map]
          associations = eo[:associations]
          parent_map = {}
          children_map = {}
          eo[:rows].each do |obj|
            parent_map[obj[prkey]] = obj
            obj.associations[descendants] = []
            obj.associations[childrena] = []
          end
          r = model.association_reflection(descendants)
          base_case = model.filter(key=>id_map.keys).
           select(SQL::AliasedExpression.new(key, ka), *c_all)
          recursive_case = model.join(t, prkey=>key).
           select(SQL::QualifiedIdentifier.new(t, ka), *c_all)
          if c = r[:conditions]
            (base_case, recursive_case) = [base_case, recursive_case].collect do |ds|
              (c.is_a?(Array) && !Sequel.condition_specifier?(c)) ? ds.filter(*c) : ds.filter(c)
            end
          end
          if associations.is_a?(Integer)
            level = associations
            no_cache_level = level - 1
            associations = {}
            base_case = base_case.select_more(SQL::AliasedExpression.new(0, la))
            recursive_case = recursive_case.select_more(SQL::AliasedExpression.new(SQL::QualifiedIdentifier.new(t, la) + 1, la)).filter(SQL::QualifiedIdentifier.new(t, la) < level - 1)
          end
          table_alias = model.dataset.schema_and_table(model.table_name)[1].to_sym
          elds = model.eager_loading_dataset(r,
           model.from(SQL::AliasedExpression.new(t, table_alias)).with_recursive(t, base_case, recursive_case,
            :args=>(([ka] + col_aliases + (level ? [la] : [])) if col_aliases)),
           r.select,
           associations, eo)
          elds = elds.select_append(ka) unless elds.opts[:select] == nil
          elds.all do |obj|
            if level
              no_cache = no_cache_level == obj.values.delete(la)
            end
            
            opk = obj[prkey]
            if parent_map.has_key?(opk)
              if idm_obj = parent_map[opk]
                idm_obj.values[ka] = obj.values[ka]
                obj = idm_obj
              end
            else
              obj.associations[childrena] = [] unless no_cache
              parent_map[opk] = obj
            end
            
            kv = obj.values.delete(ka)
            kv = kv.to_i if conv_bd && kv.is_a?(BigDecimal)
            if root = id_map[kv].first
              root.associations[descendants] << obj
            end
            
            (children_map[obj[key]] ||= []) << obj
          end
          children_map.each do |parent_id, objs|
            parent_map[parent_id].associations[childrena] = objs.uniq
          end
        end
        model.one_to_many descendants, d
      end
    end
  end
end
