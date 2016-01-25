# frozen-string-literal: true
#
# This only exists for backwards compatibility, as the behavior
# added by this extension is now the default Sequel behavior. 
Sequel::Dataset.register_extension(:empty_array_ignore_nulls){}
