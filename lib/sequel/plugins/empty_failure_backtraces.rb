# frozen-string-literal: true

module Sequel
  module Plugins
    # The empty_failure_backtraces plugin uses empty backtraces when raising HookFailed and ValidationFailed
    # exceptions.  This can be significantly faster, and if you are using these exceptions for
    # flow control, you do not need the backtraces.  This plugin is about 10% faster on CRuby
    # and 10-15x faster on JRuby 9.2.7.0+.  This does not have an effect on JRuby <9.2.7.0.
    #
    # Usage:
    #
    #   # Make all model subclass instances use empty backtraces for HookFailed
    #   # and ValidationFailed exceptions (called before loading subclasses)
    #   Sequel::Model.plugin :empty_failure_backtraces
    #
    #   # Make the Album class use empty backtraces for HookFailed and ValidationFailed exceptions
    #   Album.plugin :empty_failure_backtraces
    module EmptyFailureBacktraces
      module InstanceMethods
        private

        # Use empty backtrace for HookFailed exceptions.
        def hook_failed_error(msg)
          e = super
          e.set_backtrace([])
          e
        end

        # Use empty backtrace for ValidationFailed exceptions.
        def validation_failed_error
          e = super
          e.set_backtrace([])
          e
        end
      end
    end
  end
end
