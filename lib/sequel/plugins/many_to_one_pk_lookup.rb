module Sequel
  module Plugins
    # This is a fairly simple plugin that modifies the internal association loading logic
    # for many_to_one associations to use a simple primary key lookup on the associated
    # class, which is generally faster as it uses mostly static SQL.  Additional, if the
    # associated class is caching primary key lookups, you get the benefit of a cached
    # lookup.
    #
    # This plugin is generally not as fast as the prepared_statements_associations plugin
    # in the case where the model is not caching primary key lookups, however, it is
    # probably significantly faster if the model is caching primary key lookups.  If
    # the prepared_statements_associations plugin has been loaded first, this
    # plugin will only use the primary key lookup code if the associated model is
    # caching primary key lookups.
    #
    # This plugin attempts to determine cases where the primary key lookup would have
    # different results than the regular lookup, and use the regular lookup in that case,
    # but it cannot handle all situations correctly, which is why it is not Sequel's
    # default behavior.
    #
    # You can disable primary key lookups on a per association basis with this
    # plugin using the :many_to_one_pk_lookup=>false association option.
    # 
    # Usage:
    #
    #   # Make all model subclass instances use primary key lookups for many_to_one
    #   # association loading
    #   Sequel::Model.plugin :many_to_one_pk_lookup
    #
    #   # Do so for just the album class.
    #   Album.plugin :many_to_one_pk_lookup
    module ManyToOnePkLookup
      module InstanceMethods
        private

        # If the current association is a fairly simple many_to_one association, use
        # a simple primary key lookup on the associated model, which can benefit from
        # caching if the associated model is using caching.
        def _load_associated_object(opts, dynamic_opts)
          klass = opts.associated_class
          cache_lookup = opts.send(:cached_fetch, :many_to_one_pk_lookup) do 
            opts[:type] == :many_to_one &&
              opts[:key] &&
              opts.primary_key == klass.primary_key
          end
          if cache_lookup &&
            !dynamic_opts[:callback] &&
            (o = klass.send(:primary_key_lookup, ((fk = opts[:key]).is_a?(Array) ? fk.map{|c| send(c)} : send(fk))))
            o
          else
            super
          end
        end

        # Deal with the situation where the prepared_statements_associations plugin is
        # loaded first, by using a primary key lookup for many_to_one associations if
        # the associated class is using caching, and using the default code otherwise.
        # This is done because the prepared_statements_associations code is probably faster
        # than the primary key lookup this plugin uses if the model is not caching lookups,
        # but probably slower if the model is caching lookups.
        def _load_associated_objects(opts, dynamic_opts={})
          if opts.can_have_associated_objects?(self) && opts[:type] == :many_to_one && opts.associated_class.respond_to?(:cache_get_pk)
            _load_associated_object(opts, dynamic_opts)
          else
            super
          end
        end
      end
    end
  end
end
