module Sequel
  module Plugins
    # StringStripper is a very simple plugin that strips all input strings
    # when assigning to the model's values. Example:
    #
    #   album = Album.new(:name=>' A ')
    #   album.name # => 'A'
    # 
    # Usage:
    #
    #   # Make all model subclass instances strip strings (called before loading subclasses)
    #   Sequel::Model.plugin :string_stripper
    #
    #   # Make the Album class strip strings
    #   Album.plugin :string_stripper
    module StringStripper
      module InstanceMethods
        # Strip value if it is a string, before attempting to assign
        # it to the model's values.
        def []=(k, v)
          v.is_a?(String) ? super(k, (v.strip rescue v)) : super
        end
      end
    end
  end
end
