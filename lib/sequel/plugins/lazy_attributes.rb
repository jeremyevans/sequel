# frozen-string-literal: true

module Sequel
  module Plugins
    # The lazy_attributes plugin allows users to easily set that some attributes
    # should not be loaded by default when loading model objects.  If the attribute
    # is needed after the instance has been retrieved, a database query is made to
    # retreive the value of the attribute.
    #
    # This plugin depends on the tactical_eager_loading plugin, and allows you to
    # eagerly load lazy attributes for all objects retrieved with the current object.
    # So the following code should issue one query to get the albums and one query to
    # get the reviews for all of those albums:
    #
    #   Album.plugin :lazy_attributes, :review
    #   Album.filter{id<100}.all do |a|
    #     a.review
    #   end
    #
    #   # You can specify multiple columns to lazily load:
    #   Album.plugin :lazy_attributes, :review, :tracklist
    #
    # You may also specify groups of lazy attributes that will be retrieved together
    # when any one of them are accessed.  For instance:
    #
    #   Album.plugin :lazy_attributes, [:review, :acknowledgements], :cover_art
    #   Album.filter{id<100}.all do |a|
    #     # retrieves the "review" and "acknowledgements" for all albums in this dataset
    #     data = {:review => a.review}
    #
    #     # no additional query done here
    #     data[:acknowledgements] = a.acknowledgements
    #
    #     # this will lazy load the "cover_art" for all albums in the dataset
    #     data[:cover_art] = a.cover_art
    #   end
    #
    # Note that by default on databases that supporting RETURNING,
    # using explicit column selections will cause instance creations
    # to use two queries (insert and refresh) instead of a single
    # query using RETURNING.  You can use the insert_returning_select
    # plugin to automatically use RETURNING for instance creations
    # for models using the lazy_attributes plugin.
    module LazyAttributes
      # Lazy attributes requires the tactical_eager_loading plugin
      def self.apply(model, *attrs)
        model.plugin :tactical_eager_loading  
      end
      
      # Set the attributes given as lazy attributes
      def self.configure(model, *attrs)
        model.lazy_attributes(*attrs) unless attrs.empty?
      end
      
      module ClassMethods
        # Module to store the lazy attribute getter methods, so they can
        # be overridden and call super to get the lazy attribute behavior
        attr_accessor :lazy_attributes_module

        # Remove the given attributes from the list of columns selected by default.
        # For each attribute given, create an accessor method that allows a lazy
        # lookup of the attribute.  Attributes may be given either as a symbol or as an array
        # of symbols that will be fetched together as a group.
        def lazy_attributes(*attrs)
          unless select = dataset.opts[:select]
            select = dataset.columns.map{|c| Sequel.qualify(dataset.first_source, c)}
          end
          set_dataset(dataset.select(*select.reject{|c| attrs.flatten.include?(dataset.send(:_hash_key_symbol, c))}))
          attrs.each{|a| define_lazy_attribute_getters(a)}
        end
        
        private

        # Add lazy attribute getter methods to the lazy_attributes_module. Options:
        # :dataset :: The base dataset to use for the lazy attribute lookup
        # :table :: The table name to use to qualify the attribute and primary key columns.
        def define_lazy_attribute_getters(group, opts=OPTS)
          group = [group] unless group.is_a?(Array)
          include(self.lazy_attributes_module ||= Module.new) unless lazy_attributes_module
          group.each do |a|
            lazy_attributes_module.class_eval do
              define_method(a) do
                if !values.has_key?(a) && !new?
                  # no point in retrieving all the values in the group if the object is frozen
                  # since they won't be persisted anywhere
                  lazy_lookups = frozen? ? [a] : group.reject { |col| values.has_key?(col) }
                  lazy_values_hash = lazy_attribute_lookup(lazy_lookups, opts)
                  lazy_values_hash[a]
                else
                  super()
                end
              end
            end
          end
        end
        # for back compat...
        alias_method :define_lazy_attribute_getter, :define_lazy_attribute_getters
      end

      module InstanceMethods
        private

        # If the model was selected with other model objects, eagerly load the
        # group of attributes for all of those objects.  If not, query the database for
        # the group of attributes for just the current object.  Return a hash of values for
        # the group of attributes for the current object keyed by the column names.
        def lazy_attribute_lookup(group, opts=OPTS)
          unless table = opts[:table]
            table = model.table_name
          end

          if base_ds = opts[:dataset]
            ds = base_ds.where(qualified_pk_hash(table))
          else
            base_ds = model.dataset
            ds = this
          end

          selection = group.map { |a| Sequel.qualify(table, a) }

          if frozen?
            lazy_values = ds.dup.get(selection)
            return Hash[group.zip(lazy_values)]
          end

          if retrieved_with
            raise(Error, "Invalid primary key column for #{model}: #{pkc.inspect}") unless primary_key = model.primary_key
            composite_pk = true if primary_key.is_a?(Array)
            id_map = {}
            retrieved_with.each{|o| id_map[o.pk] = o unless group.all? { |a| o.values.has_key?(a) } || o.frozen?}
            predicate_key = composite_pk ? primary_key.map{|k| Sequel.qualify(table, k)} : Sequel.qualify(table, primary_key)
            base_ds.select(*(Array(primary_key).map{|k| Sequel.qualify(table, k)} + selection)).where(predicate_key=>id_map.keys).naked.each do |row|
              obj = id_map[composite_pk ? row.values_at(*primary_key) : row[primary_key]]
              if obj
                group.each do |a|
                  if !obj.values.has_key?(a)
                    obj.values[a] = row[a]
                  end
                end
              end
            end
          end
          if group.all? { |a| values.has_key?(a) }
            lazy_values = values.values_at(*group)
            lazy_values_hash = Hash[group.zip(lazy_values)]
          else
            lazy_values = ds.get(selection)
            lazy_values_hash = Hash[group.zip(lazy_values)]
            values.merge!(lazy_values_hash) { |k, old, new| old }
          end
          lazy_values_hash
        end
      end
    end
  end
end
