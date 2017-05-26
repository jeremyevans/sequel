# frozen-string-literal: true

Sequel::Deprecation.deprecate("The many_to_one_pk_lookup plugin", "This plugin was integrated into the default model behavior in Sequel 4.0, and no longer has an effect")

module Sequel
  module Plugins
    # Empty plugin module for backwards compatibility
    module ManyToOnePkLookup
    end
  end
end
