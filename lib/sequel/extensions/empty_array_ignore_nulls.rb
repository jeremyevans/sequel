# frozen-string-literal: true
#
# This only exists for backwards compatibility, as the behavior
# added by this extension is now the default Sequel behavior. 

Sequel::Deprecation.deprecate("The empty_array_ignore_nulls", "It has been a no-op since 4.25.0")

Sequel::Dataset.register_extension(:empty_array_ignore_nulls){}
