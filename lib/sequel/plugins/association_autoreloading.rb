# frozen-string-literal: true

Sequel::Deprecation.deprecate("The association_autoreloading plugin", "This plugin was integrated into the default model behavior in Sequel 4.0, and no longer has an effect")

module Sequel
  module Plugins
    # Empty plugin module for backwards compatibility
    module AssociationAutoreloading
    end
  end
end
