# frozen-string-literal: true

module Sequel
  module Plugins
    # The deprecated_associations plugin adds support for
    # deprecating associations. Attempts to use association
    # methods and access association metadata for deprecated
    # associations results in a warning.
    #
    #   Album.plugin :deprecated_associations
    #   Album.many_to_one :artist, deprecated: true
    #   album = Album[1]
    #
    #   # Warnings for all of the following calls
    #   album.artist
    #   album.artist_dataset
    #   album.artist = Artist[2]
    #   Album.association_reflection(:artist)
    #   Album.eager(:artist)
    #   Album.eager_graph(:artist)
    #   Album.where(artist: Artist[1]).all
    #
    # By default, the plugin issues a single warning per
    # association method or association reflection. See
    # DeprecatedAssociations.configure for options to make
    # deprecated association issue warnings for every access,
    # to include backtraces in warnings, or to raise an
    # exception instead of warning.
    #
    # Note that Model.association_reflections and
    # Model.all_association_reflections will include deprecated
    # associations, and accessing the metadata for deprecated
    # associations through these interfaces not issue warnings.
    #
    # Usage:
    #
    #   # Make all models support deprecated associations
    #   # (called before loading subclasses)
    #   Sequel::Model.plugin :deprecated_associations
    #
    #   # Make Album class support deprecated associations
    #   Album.plugin :deprecated_associations
    module DeprecatedAssociations
      # Exception class used for deprecated association use when
      # raising exceptions instead of emitting warnings.
      class Access < Sequel::Error; end

      # Configure the deprecated associations plugin. Options:
      #
      # backtrace: Print backtrace with warning
      # deduplicate: Set to false to emit warnings for every
      #              deprecated association method call/access
      #              (when not caching associations, this is always false)
      # raise: Raise Sequel::Plugin::DeprecatedAssociations::Access
      #        instead of warning
      def self.configure(model, opts = OPTS)
        model.instance_exec do
          (@deprecated_associations_config ||= {}).merge!(opts)
        end
      end

      module ClassMethods
        # Issue a deprecation warning if the association is deprecated.
        def association_reflection(assoc)
          ref = super
          if ref && ref[:deprecated]
            emit_deprecated_association_warning(ref, nil) do
              "Access of association reflection for deprecated association: class:#{name} association:#{assoc}"
            end
          end
          ref
        end

        private

        # Issue a deprecation warning when the defined method is called if the
        # association is deprecated and the method name does not start with the
        # underscore (to avoid not warning twice, once for the public association
        # method and once for the private association method).
        def association_module_def(name, opts=OPTS, &block)
          super
          if opts[:deprecated] && name[0] != "_"
            deprecated_associations_module.module_exec do
              define_method(name) do |*a, &b|
                self.class.send(:emit_deprecated_association_warning, opts, name) do
                  "Calling deprecated association method: class:#{self.class.name} association:#{opts[:name]} method:#{name}"
                end
                super(*a, &b)
              end
              alias_method name, name
            end
          end
          nil
        end

        # Issue a deprecation warning when the defined method is called if the
        # association is deprecated.
        def association_module_delegate_def(name, opts, &block)
          super
          if opts[:deprecated]
            deprecated_associations_module.module_exec do
              define_method(name) do |*a, &b|
                self.class.send(:emit_deprecated_association_warning, opts, name) do
                  "Calling deprecated association method: class:#{self.class.name} association:#{opts[:name]} method:#{name}"
                end
                super(*a, &b)
              end
              # :nocov:
              ruby2_keywords(name) if respond_to?(:ruby2_keywords, true)
              # :nocov:
              alias_method(name, name)
            end
          end
          nil
        end

        # A module to add deprecated association methods to. These methods
        # handle issuing the deprecation warnings, and call super to get the
        # default behavior.
        def deprecated_associations_module
          return @deprecated_associations_module if defined?(@deprecated_associations_module)
          @deprecated_associations_module = Module.new
          include(@deprecated_associations_module)
          @deprecated_associations_module
        end

        # Emit a deprecation warning, or raise an exception if the :raise
        # plugin option was used.
        def emit_deprecated_association_warning(ref, method)
          config = @deprecated_associations_config

          raise Access, yield if config[:raise]

          unless config[:deduplicate] == false
            emit = false
            ref.send(:cached_fetch, [:deprecated_associations, method]) do
              emit = true
            end
            return unless emit
          end

          if config[:backtrace]
            warn yield, caller(2)
          else
            warn yield, :uplevel => 2
          end
        end
      end
    end
  end
end
