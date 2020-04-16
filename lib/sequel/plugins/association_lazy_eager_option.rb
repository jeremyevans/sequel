# frozen-string-literal: true

module Sequel
  module Plugins
    # The association_lazy_eager_option plugin supports passing
    # an +:eager+ option to an association method.  If the related
    # association is already cached, the cached version will be
    # returned.  If the association is not already cached, it will
    # be loaded, and the value of the +:eager+ option will be used
    # to perform an eager load of the given associations.
    # the plural versions.
    #
    # With Sequel's default behavior, you can already perform an
    # eager load when lazy loading using a block:
    #
    #   obj.association{|ds| ds.eager(:nested_association)}
    #
    # However, this will ignore any cached version.  In more
    # complex software, the association may already be cached
    # and have the nested association cached inside of it, and
    # using this callback approach then requires 2 unnecessary
    # queries. This plugin will not perform any queries if the
    # association is already cached, preventing duplicate work.
    # However, you should make sure that an already loaded
    # association has the nested association already eagerly
    # loaded.
    # 
    # Usage:
    #
    #   # Make all model subclasses support the :eager association
    #   # method option (called before loading subclasses)
    #   Sequel::Model.plugin :association_lazy_eager_option
    #
    #   # Make the Album class support the :eager association
    #   # method option
    #   Album.plugin :association_lazy_eager_option
    module AssociationLazyEagerOption
      module InstanceMethods
        private

        # Return a dataset for the association after applying any dynamic callback.
        def _associated_dataset(opts, dynamic_opts)
          ds = super

          if eager = dynamic_opts[:eager]
            ds = ds.eager(eager)
          end

          ds
        end
        
        # A placeholder literalizer that can be used to load the association, or nil to not use one.
        def _associated_object_loader(opts, dynamic_opts)
          return if dynamic_opts[:eager]
          super
        end

        # Whether to use a simple primary key lookup on the associated class when loading.
        def load_with_primary_key_lookup?(opts, dynamic_opts)
          return false if dynamic_opts[:eager]
          super
        end
      end
    end
  end
end
