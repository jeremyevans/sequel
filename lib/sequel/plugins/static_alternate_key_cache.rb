module Sequel
  module Plugins
    # The static_alternate_key_cache plugin is designed for models that are not modified at all
    # in production use cases, or at least where modifications to them would usually
    # coincide with an application restart.  When loaded into a model class, it
    # retrieves all rows in the database and staticly caches a ruby array and hash
    # keyed on the (unique) alternate key attribute(s) specified containing all of the model instances.
    # All of these instances are frozen so they won't be modified unexpectedly.
    #
    # The caches this plugin creates are used for lookups via the alternate key(s),
    # e.g. Model.by_alternate_key1('FOO'), Model.by_alternate_key2('BAR')
    #
    # Usage:
    #
    #   # Cache the AlbumType class staticly by the unique attribute `code`, where codes could be
    #   # 'LP', 'SINGLE', etc.
    #   AlbumType.plugin :static_alternate_key_cache, :code
    #   AlbumType.by_code('SINGLE')
    #
    #   # You can specify multiple attributes to use as lookup keys:
    #   AlbumType.plugin :static_alternate_key_cache, :code, :iso_code
    #   AlbumType.by_code('SINGLE')
    #   AlbumType.by_iso_code('ISO_SINGLE')
    module StaticAlternateKeyCache
      # Populate the static caches when loading the plugin.
      def self.configure(model, *attrs)
        model.send(:load_alternate_key_cache, *attrs)
      end

      module ClassMethods
        # A frozen ruby hash of hashes holding all of the model's frozen instances, keyed first by lookup
        # attribute and then attribute-specific keys.
        attr_reader :alternate_key_cache

        # Reload the cache when the dataset changes.
        def set_dataset(*)
          s = super
          k = alternate_key_cache ? alternate_key_cache.keys : []
          load_alternate_key_cache(*k)
          s
        end


        private

        # Reload the cache for this model by retrieving all of the instances in the dataset
        # freezing them, populating the hash cache, and defining class methods of the form by_[attribute].
        def load_alternate_key_cache(*attrs)
          if attrs.empty?
            @alternate_key_cache = {}
            return
          end

          objects = dataset.all
          lookups = {}
          attrs.each { |attr| lookups[attr] = {} }
          objects.each do |o|
            o.freeze
            attrs.each { |attr| lookups[attr][o.send(attr).freeze] = o }
          end
          lookups.values.each { |v| v.freeze }
          @alternate_key_cache = lookups.freeze

          metaclass = class << self; self; end
          attrs.each do |attr|
            metaclass.instance_eval do
              define_method("by_#{attr}") { |v| @alternate_key_cache[attr][v] }
            end
          end
        end
      end
    end
  end
end
