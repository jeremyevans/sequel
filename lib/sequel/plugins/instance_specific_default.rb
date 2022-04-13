# frozen-string-literal: true

module Sequel
  module Plugins
    # The instance_specific_default plugin exists to make it easier to use a
    # global :instance_specific association option, or to warn or raise when Sequel
    # has to guess which value to use :instance_specific option (Sequel defaults to
    # guessing true as that is the conservative setting).  It is helpful to
    # use this plugin, particularly with the :warn or :raise settings, to determine
    # which associations should have :instance_specific set.  Setting the
    # :instance_specific to false for associations that are not instance specific
    # can improve performance.
    #
    # Associations are instance-specific if their block calls
    # a model instance method, or where the value of the block varies
    # based on runtime state, and the variance is outside of a delayed evaluation.
    # For example, with the following three associations:
    #
    #   Album.one_to_one :first_track, class: :Track do |ds|
    #     ds.where(number: 1)
    #   end
    #
    #   Album.one_to_one :last_track, class: :Track do |ds|
    #     ds.where(number: num_tracks)
    #   end
    #
    #   Album.one_to_many :recent_tracks, class: :Track do |ds|
    #     ds.where{date_updated > Date.today - 10}
    #   end
    #
    # +first_track+ is not instance specific, but +last_track+ and +recent_tracks+ are.
    # +last_track+ is because the +num_tracks+ call in the block is calling
    # <tt>Album#num_tracks</tt>.  +recent_tracks+ is because the value will change over
    # time. This plugin allows you to find these cases, and set the :instance_specific
    # option appropriately for them:
    #
    #   Album.one_to_one :first_track, class: :Track, instance_specific: false do |ds|
    #     ds.where(number: 1)
    #   end
    #
    #   Album.one_to_one :last_track, class: :Track, instance_specific: true do |ds|
    #     ds.where(number: num_tracks)
    #   end
    #
    #   Album.one_to_many :recent_tracks, class: :Track, instance_specific: true do |ds|
    #     ds.where{date_updated > Date.today - 10}
    #   end
    #
    # For the +recent_tracks+ association, instead of marking it instance_specific, you
    # could also use a delayed evaluation, since it doesn't actually contain
    # instance-specific code:
    #
    #   Album.one_to_many :recent_tracks, class: :Track, instance_specific: false do |ds|
    #     ds.where{date_updated > Sequel.delay{Date.today - 10}}
    #   end
    #
    # Possible arguments to provide when loading the plugin:
    #
    # true :: Set the :instance_specific option to true
    # false :: Set the :instance_specific option to false
    # :default :: Call super to set the :instance_specific option
    # :warn :: Emit a warning before calling super to set the :instance_specific option
    # :raise :: Raise a Sequel::Error if an :instance_specific option is not provided and
    #           an association could be instance-specific.
    #
    # Note that this plugin only affects associations which could be instance
    # specific (those with blocks), where the :instance_specific option was not
    # specified when the association was created.
    #
    # Usage:
    #
    #   # Set how to handle associations that could be instance specific
    #   # but did not specify an :instance_specific option, for all subclasses
    #   # (set before creating subclasses).
    #   Sequel::Model.plugin :instance_specific_default, :warn
    #
    #   # Set how to handle associations that could be instance specific
    #   # but did not specify an :instance_specific option, for the Album class
    #   Album.plugin :instance_specific_default, :warn
    module InstanceSpecificDefault
      # Set how to handle associations that could be instance specific but did
      # not specify an :instance_specific value.
      def self.configure(model, default)
        model.instance_variable_set(:@instance_specific_default, default)
      end

      module ClassMethods
        Plugins.inherited_instance_variables(self, :@instance_specific_default=>nil)

        private

        # Return the appropriate :instance_specific value, or warn or raise if
        # configured.
        def _association_instance_specific_default(name)
          case @instance_specific_default
          when true, false
            return @instance_specific_default
          when :default
            # nothing
          when :warn
            warn("possibly instance-specific association without :instance_specific option (class: #{self}, association: #{name})", :uplevel => 3)
          when :raise
            raise Sequel::Error, "possibly instance-specific association without :instance_specific option (class: #{self}, association: #{name})"
          else
            raise Sequel::Error, "invalid value passed to instance_specific_default plugin: #{@instance_specific_default.inspect}"
          end

          super
        end
      end
    end
  end
end
