# frozen-string-literal: true

module Sequel
  module Plugins
    # The forbid_lazy_load plugin forbids lazy loading of associations
    # for objects in cases where the object wasn't loaded with a
    # method that only returns a single object.
    #
    # The main reason for doing this is it makes it easier to detect
    # N+1 query issues. Note that Sequel also offers a
    # tactical_eager_loading plugin which will automatically eagerly
    # load associations for all objects retrived in the same query
    # if any object would attempt to lazily load an association. That
    # approach may be simpler if you are trying to prevent N+1 issues,
    # though it does retain more objects in memory.
    #
    # This plugin offers multiple different ways to forbid lazy
    # loading.  You can forbid lazy loading associations for individual
    # model instances:
    #
    #   obj = Album[1]
    #   obj.forbid_lazy_load
    #   obj.artist # raises Sequel::Plugins::ForbidLazyLoad::Error
    #
    # +forbid_lazy_load+ is automatically called on instances if the
    # instances are loaded via a method such as Dataset#all,
    # Dataset#each, and other methods that load multiple instances
    # at once.  These are the cases where lazily loading associations
    # for such instances can cause N+1 issues.
    #
    #   Album.all.first.artist
    #   objs.first.artist # raises Sequel::Plugins::ForbidLazyLoad::Error
    #
    #   Album.each do |obj|
    #     obj.artist # raises Sequel::Plugins::ForbidLazyLoad::Error
    #   end
    #
    #   Album[1].artist # no error
    #
    #   Album.first.artist # no error
    #
    # This behavior of enabling +forbid_lazy_load+ automatically from dataset
    # methods can be disabled using the plugin's +:allow_by_default+ option.
    #
    # You can allow lazy loading associations for an instance that it
    # was previously forbidden for:
    # 
    #   obj = Album.all.first
    #   obj.allow_lazy_load
    #   obj.artist # no error
    #
    # You can forbid lazy loading associations on a per-call basis,
    # even if lazy loading of associations is allowed for the instance:
    #
    #   obj = Album[1]
    #   obj.artist(forbid_lazy_load: true)
    #   # raises Sequel::Plugins::ForbidLazyLoad::Error
    #
    # This also works for allowing lazy loading associations for a
    # specific association load even if it is forbidden for the instance:
    #  
    #   obj = Album.all.first
    #   obj.artist(forbid_lazy_load: false)
    #   # nothing raised
    #
    # You can also forbid lazy loading on a per-association basis using the
    # +:forbid_lazy_load+ association option with a +true+ value:
    #
    #   Album.many_to_one :artist, forbid_lazy_load: true
    #   Album[1].artist # raises Sequel::Plugins::ForbidLazyLoad::Error
    #
    # However, you probably don't want to do this as it will forbid any
    # lazy loading of the association, even if the loading could not
    # result in an N+1 issue.
    #
    # On the flip side, you can allow lazy loading using the 
    # +:forbid_lazy_load+ association option with a +false+ value:
    #
    #   Album.many_to_one :artist, forbid_lazy_load: false
    #   Album.all.first.artist # no error
    #
    # One reason to do this is when using a plugin like static_cache
    # on the associated model, where a query is not actually issued
    # when doing a lazy association load.  To make that particular
    # case easier, this plugin makes Model.finalize_associations
    # automatically set the association option if the associated
    # class uses the static_cache plugin.
    #
    # Note that even with this plugin, there can still be N+1 issues,
    # such as:
    #
    #   Album.each do |obj| # 1 query for all albums
    #     Artist[obj.artist_id] # 1 query per album for each artist
    #   end
    # 
    # Usage:
    #
    #   # Make all model subclasses support forbidding lazy load
    #   # (called before loading subclasses)
    #   Sequel::Model.plugin :forbid_lazy_load
    #
    #   # Make the Album class support forbidding lazy load
    #   Album.plugin :forbid_lazy_load
    #
    #   # Let lazy loading be forbidden by object, but not automatically for any
    #   # object loaded via dataset.
    #   Album.plugin :forbid_lazy_load, allow_by_default: true
    module ForbidLazyLoad
      def self.apply(model, opts=OPTS)
        unless opts[:allow_by_default]
          model.send(:dataset_extend, ForbidByDefault, :create_class_methods=>false)
        end
      end

      # Error raised when attempting to lazy load an association when
      # lazy loading has been forbidden.
      class Error < StandardError
      end

      module ClassMethods
        Plugins.def_dataset_methods(self, :forbid_lazy_load)

        # If the static_cache plugin is used by the associated class for
        # an association, allow lazy loading that association, since the
        # lazy association load will use a hash table lookup and not a query.
        def allow_lazy_load_for_static_cache_associations
          # :nocov:
          if defined?(::Sequel::Plugins::StaticCache::ClassMethods)
          # :nocov:
            @association_reflections.each_value do |ref|
              if ref.associated_class.is_a?(::Sequel::Plugins::StaticCache::ClassMethods)
                ref[:forbid_lazy_load] = false
              end
            end
          end
        end

        # Allow lazy loading for static cache associations before finalizing.
        def finalize_associations
          allow_lazy_load_for_static_cache_associations
          super
        end
      end

      module InstanceMethods
        # Set this model instance to allow lazy loading of associations.
        def allow_lazy_load
          @forbid_lazy_load = false
          self
        end

        # Set this model instance to not allow lazy loading of associations.
        def forbid_lazy_load
          @forbid_lazy_load = true
          self
        end

        private

        # Allow lazy loading for objects returned by singular associations.
        def _load_associated_object(opts, dynamic_opts)
          # The implementation that loads these associations does
          # .all.first, which would result in the object returned being
          # marked as forbidding lazy load.
          obj = super
          obj.allow_lazy_load if obj.is_a?(InstanceMethods)
          obj
        end

        # Raise an Error if lazy loading has been forbidden for
        # the instance, association, or call.
        def _load_associated_objects(opts, dynamic_opts=OPTS)
          case dynamic_opts[:forbid_lazy_load]
          when false
            # nothing
          when nil
            unless dynamic_opts[:reload]
              case opts[:forbid_lazy_load]
              when nil
                raise Error, "lazy loading forbidden for this object (association: #{opts.inspect}, object: #{inspect})" if @forbid_lazy_load
              when false
                # nothing
              else
                raise Error, "lazy loading forbidden for this association (#{opts.inspect})"
              end
            end
          else
            raise Error, "lazy loading forbidden for this association method call (association: #{opts.inspect})"
          end

          super
        end
      end

      module ForbidByDefault
        # Mark model instances retrieved in this call as forbidding lazy loading.
        def each
          if row_proc
            super do |obj|
              obj.forbid_lazy_load if obj.is_a?(InstanceMethods)
              yield obj
            end
          else
            super
          end
        end

        # Mark model instances retrieved in this call as forbidding lazy loading.
        def with_sql_each(sql)
          if row_proc
            super(sql) do |obj|
              obj.forbid_lazy_load if obj.is_a?(InstanceMethods)
              yield obj
            end
          else
            super
          end
        end

        # Mark model instances retrieved in this call as allowing lazy loading.
        def with_sql_first(sql)
          obj = super
          obj.allow_lazy_load if obj.is_a?(InstanceMethods)
          obj
        end
      end
    end
  end
end
