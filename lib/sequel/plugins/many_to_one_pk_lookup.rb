module Sequel
  module Plugins
    # The ManyToOnePkLookup plugin that modifies the internal association loading logic
    # for many_to_one associations to use a simple primary key lookup on the associated
    # class, which is generally faster as it uses mostly static SQL.  Additional, if the
    # associated class is caching primary key lookups, you get the benefit of a cached
    # lookup.
    #
    # This plugin attempts to determine cases where the primary key lookup would have
    # different results than the regular lookup, and use the regular lookup in that case.
    # If you want to explicitly force whether or not to use primary key lookups for
    # a given association, set the :many_to_one_pk_lookup association option.
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
      module ClassMethods
        # Disable primary key lookup in cases where it will result in a different
        # query than the association query.
        def def_many_to_one(opts)
          if !opts.has_key?(:many_to_one_pk_lookup) &&
             (opts[:dataset] || opts[:conditions] || opts[:block] || opts[:select] ||
              (opts.has_key?(:key) && opts[:key] == nil))
            opts[:many_to_one_pk_lookup] = false
          end
          super
        end
      end

      module InstanceMethods
        private

        # If the current association is a simple many_to_one association, use
        # a simple primary key lookup on the associated model, which can benefit from
        # caching if the associated model is using caching.
        def _load_associated_objects(opts, dynamic_opts={})
          return super unless opts.can_have_associated_objects?(self) && opts[:type] == :many_to_one
          klass = opts.associated_class
          if !dynamic_opts[:callback] &&
             opts.send(:cached_fetch, :many_to_one_pk_lookup){opts.primary_key == klass.primary_key}
            klass.send(:primary_key_lookup, ((fk = opts[:key]).is_a?(Array) ? fk.map{|c| send(c)} : send(fk)))
          else
            super
          end
        end
      end
    end
  end
end
