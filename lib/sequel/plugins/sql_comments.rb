# frozen-string-literal: true

module Sequel
  module Plugins
    # The sql_comments plugin will automatically use SQL comments on
    # queries for the model it is loaded into.  These comments will
    # show the related model, what type of method was called, and
    # the method name (or association name for queries to load
    # associations):
    #
    #   album = Album[1]
    #   # SELECT * FROM albums WHERE (id = 1) LIMIT 1
    #   # -- model:Album,method_type:class,method:[]
    #
    #   album.update(name: 'A')
    #   # UPDATE albums SET name = 'baz' WHERE (id = 1)
    #   # -- model:Album,method_type:instance,method:update
    #
    #   album.artist
    #   # SELECT * FROM artists WHERE (artists.id = 1)
    #   # -- model:Album,method_type:association_load,association:artist
    #
    #   Album.eager(:artists).all
    #   # SELECT * FROM albums
    #   # SELECT * FROM artists WHERE (artists.id IN (1))
    #   # -- model:Album,method_type:association_eager_load,association:artist
    #   
    #   Album.where(id: 1).delete
    #   # DELETE FROM albums WHERE (id = 1)
    #   # -- model:Album,method_type:dataset,method:delete
    #
    # This plugin automatically supports the class, instance, and dataset
    # methods are are supported by default in Sequel::Model.  To support
    # custom class, instance, and dataset methods, such as those added by
    # other plugins, you can use the appropriate <tt>sql_comments_*_methods</tt>
    # class method:
    #
    #   Album.sql_comments_class_methods :first_by_name # example from finder plugin, with :mod option
    #   Album.sql_comments_instance_methods :lazy_attribute_lookup # lazy_attributes plugin
    #   Album.sql_comments_dataset_methods :to_csv # csv_serializer plugin
    #
    # In order for the sql_comments plugin to work, the sql_comments
    # Database extension must be loaded into the model's database, so
    # loading the plugin does this automatically.
    #
    # Note that in order to make sure SQL comments are included, some
    # optimizations are disabled if this plugin is loaded.
    # 
    # Usage:
    #
    #   # Make all model subclasses support automatic SQL comments
    #   # (called before loading subclasses)
    #   Sequel::Model.plugin :sql_comments
    #
    #   # Make the Album class support automatic SQL comments
    #   Album.plugin :sql_comments
    module SqlComments
      # Define a method +meth+ on the given module +mod+ that will use automatic
      # SQL comments with the given model, method_type, and method.
      def self.def_sql_commend_method(mod, model, method_type, meth)
        mod.send(:define_method, meth) do |*a, &block|
          model.db.with_comments(:model=>model, :method_type=>method_type, :method=>meth) do
            super(*a, &block)
          end
        end
        # :nocov:
        mod.send(:ruby2_keywords, meth) if mod.respond_to?(:ruby2_keywords, true)
        # :nocov:
      end

      def self.apply(model)
        model.db.extension(:sql_comments)
      end

      def self.configure(model)
        model.send(:reset_fast_pk_lookup_sql)
      end

      module ClassMethods
        # Use automatic SQL comments for the given class methods.
        def sql_comments_class_methods(*meths)
          _sql_comments_methods(singleton_class, :class, meths)
        end

        # Use automatic SQL comments for the given instance methods.
        def sql_comments_instance_methods(*meths)
          _sql_comments_methods(self, :instance, meths)
        end

        # Use automatic SQL comments for the given dataset methods.
        def sql_comments_dataset_methods(*meths)
          unless @_sql_comments_dataset_module
            dataset_module(@_sql_comments_dataset_module = Sequel.set_temp_name(Module.new){"#{name}::@_sql_comments_dataset_module"})
          end
          _sql_comments_methods(@_sql_comments_dataset_module, :dataset, meths)
        end

        [:[], :create, :find, :find_or_create, :with_pk, :with_pk!].each do |meth|
          define_method(meth) do |*a, &block|
            db.with_comments(:model=>self, :method_type=>:class, :method=>meth) do
              super(*a, &block)
            end
          end
          # :nocov:
          ruby2_keywords(meth) if respond_to?(:ruby2_keywords, true)
          # :nocov:
        end

        private

        # Don't optimize the fast PK lookups, as it uses static SQL that
        # won't support the SQL comments.
        def reset_fast_pk_lookup_sql
          @fast_pk_lookup_sql = @fast_instance_delete_sql = nil
        end

        # Define automatic SQL comment methods in +mod+ for each method in +meths+,
        # with the given +method_type+.
        def _sql_comments_methods(mod, method_type, meths)
          meths.each do |meth|
            SqlComments.def_sql_commend_method(mod, self, method_type, meth)
          end
        end
      end

      module InstanceMethods
        [:delete, :destroy, :lock!, :refresh, :save, :save_changes, :update, :update_fields].each do |meth|
          define_method(meth) do |*a, &block|
            t = Sequel.current
            return super(*a, &block) if (hash = Sequel.synchronize{db.comment_hashes[t]}) && hash[:model]

            db.with_comments(:model=>model, :method_type=>:instance, :method=>meth) do
              super(*a, &block)
            end
          end
          # :nocov:
          ruby2_keywords(meth) if respond_to?(:ruby2_keywords, true)
          # :nocov:
        end

        private

        # Do not use a placeholder loader for associations.
        def _associated_object_loader(opts, dynamic_opts)
          nil
        end

        # Use SQL comments on normal association load queries, showing they are association loads.
        def _load_associated_objects(opts, dynamic_opts=OPTS)
          db.with_comments(:model=>model, :method_type=>:association_load, :association=>opts[:name]) do
            super
          end
        end
      end

      module DatasetMethods
        Dataset::ACTION_METHODS.each do |meth|
          define_method(meth) do |*a, &block|
            t = Sequel.current
            return super(*a, &block) if (hash = Sequel.synchronize{db.comment_hashes[t]}) && hash[:model]

            db.with_comments(:model=>model, :method_type=>:dataset, :method=>meth) do
              super(*a, &block)
            end
          end
          # :nocov:
          ruby2_keywords(meth) if respond_to?(:ruby2_keywords, true)
          # :nocov:
        end

        private

        # Add the association name as part of the eager load data, so
        # perform_eager_load has access to it.
        def prepare_eager_load(a, reflections, eager_assoc)
          res = super
          
          reflections.each do |r|
            res[r[:eager_loader]][:association] = r[:name]
          end

          res
        end

        # Use SQL comments on eager load queries, showing they are eager loads.
        def perform_eager_load(loader, eo)
          db.with_comments(:model=>model, :method_type=>:association_eager_load, :method=>nil, :association=>eo[:association]) do
            super
          end
        end
      end
    end
  end
end
