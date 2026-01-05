# frozen-string-literal: true

module Sequel
  module Plugins
    # The detect_unnecessary_association_options plugin can detect unnecessary
    # association options, and either warn or raise if they are detected.
    # This allows you to find and remove the unnecessary options.
    # Association options are considered unnecessary if they specify the same
    # value as Sequel's defaults.
    #
    # To detect unnecessary association options, you should load the plugin
    # into your model base class (e.g. Sequel::Model) before loading your model
    # classes. Then, after all models have been loaded, you can call the
    # detect_unnecessary_association_options on each to check for unnecessary
    # association options. Additionally, if you are calling finalize_associations,
    # it will automatically check for unnecessary association options.
    #
    # A typical usage would be to combine this with the subclasses plugin:
    #
    #   Sequel::Model.plugin :detect_unnecessary_association_options
    #   Sequel::Model.plugin :subclasses
    #   # load model classes
    #
    #   # implicitly check all subclasses when freezing descendants
    #   Sequel::Model.freeze_descendants
    #
    #   # or, if not freezing all descendants
    #   Sequel::Model.descendants.each(&:detect_unnecessary_association_options)
    #
    # By default, the plugin warns for every unnecessary association option.
    # To raise an error instead, you can pass the <tt>action: :raise</tt> option when loading the
    # plugin:
    #
    #   Sequel::Model.plugin :detect_unnecessary_association_options, action: :raise
    #
    # This plugin only detects the most common unnecessary association options, such as:
    #
    # * :class (all associations)
    # * :key and :primary_key (associations without join tables)
    # * :join_table, :left_key, :right_key, :left_primary_key, :right_primary_key (single join table associations)
    # * :left_primary_key, :right_primary_key (*_through_many associations)
    #
    # Only association types supported by default or supported by a plugin that
    # ships with Sequel are supported by this plugin. Other association types are
    # ignored.
    module DetectUnnecessaryAssociationOptions
      def self.configure(model, opts={})
        model.instance_variable_set(:@detect_unnecessary_association_options_action, opts[:action] || :warn)
      end

      # Raised if the plugin action is to raise and an unnecessary association option
      # is detected.
      class UnnecessaryAssociationOption < Sequel::Error
      end

      module ClassMethods
        Plugins.inherited_instance_variables(self, :@detect_unnecessary_association_options_action => nil)

        # Implicitly check for unnecessary association options when finalizing associations.
        def finalize_associations
          res = super
          detect_unnecessary_association_options
          res
        end

        # Check for unnecessary association options.
        def detect_unnecessary_association_options
          @association_reflections.each_value do |ref|
            meth = "detect_unnecessary_association_options_#{ref[:type]}"
            # Expected to call private methods.
            # Ignore unrecognized association types.
            # External association types can define the appropriate method to
            # support their own unnecessary association option checks.
            if respond_to?(meth, true)
              # All recognized association types need same class check
              _detect_unnecessary_association_options_class(ref)
              send(meth, ref)
            end
          end

          nil
        end

        private

        # Action to take if an unnecessary association option is detected.
        def unnecessary_association_options_detected(ref, key)
          if @detect_unnecessary_association_options_action == :raise
            raise UnnecessaryAssociationOption, "#{ref.inspect} :#{key} option unnecessary"
          else
            warn "#{ref.inspect} :#{key} option unnecessary"
          end
        end

        # Detect unnecessary :class option.
        def _detect_unnecessary_association_options_class(ref)
          return unless ref[:orig_class]

          h = {}
          name = ref[:name]
          late_binding_class_option(h, ref.returns_array? ? singularize(name) : name)

          begin
            default_association_class = constantize(h[:class_name])
            actual_association_class = ref.associated_class
          rescue NameError
            # Do not warn. For the default association class to not be a valid
            # constant is expected. For the actual association class to not be
            # a valid constant is not expected and a bug in the association, but
            # the job of this plugin is not to detect invalid options, only
            # unnecessary options.
          else
            if default_association_class.equal?(actual_association_class)
              unnecessary_association_options_detected(ref, "class")
            end
          end
        end

        # Detect other unnecessary options. An option is considered unnecessary
        # if the key was submitted as an association option and the value for
        # the option is the same as the given value.
        def _detect_unnecessary_association_options_key_value(ref, key, value)
          if ref[:orig_opts].has_key?(key) && ref[:orig_opts][key] == value
            unnecessary_association_options_detected(ref, key)
          end
        end

        # Same as _detect_unnecessary_association_options_key_value, but calls
        # the default_* method on the association reflection to get the default value.
        def _detect_unnecessary_association_options_key(ref, key)
          _detect_unnecessary_association_options_key_value(ref, key, ref.send(:"default_#{key}"))
        end

        def detect_unnecessary_association_options_many_to_one(ref)
          _detect_unnecessary_association_options_key(ref, :key)
          _detect_unnecessary_association_options_key_value(ref, :primary_key, ref.associated_class.primary_key)
        end
        alias detect_unnecessary_association_options_pg_array_to_many detect_unnecessary_association_options_many_to_one

        def detect_unnecessary_association_options_one_to_many(ref)
          _detect_unnecessary_association_options_key(ref, :key)
          _detect_unnecessary_association_options_key_value(ref, :primary_key, primary_key)
        end
        alias detect_unnecessary_association_options_one_to_one detect_unnecessary_association_options_one_to_many
        alias detect_unnecessary_association_options_many_to_pg_array detect_unnecessary_association_options_one_to_many

        def detect_unnecessary_association_options_many_to_many(ref)
          [:join_table, :left_key, :right_key].each do |key|
            _detect_unnecessary_association_options_key(ref, key)
          end
          _detect_unnecessary_association_options_key_value(ref, :left_primary_key, primary_key)
          _detect_unnecessary_association_options_key_value(ref, :right_primary_key, ref.associated_class.primary_key)
        end
        alias detect_unnecessary_association_options_one_through_one detect_unnecessary_association_options_many_to_many

        def detect_unnecessary_association_options_many_through_many(ref)
          _detect_unnecessary_association_options_key_value(ref, :left_primary_key, primary_key)
          _detect_unnecessary_association_options_key_value(ref, :right_primary_key, ref.associated_class.primary_key)
        end
        alias detect_unnecessary_association_options_one_through_many detect_unnecessary_association_options_many_through_many
      end
    end
  end
end
