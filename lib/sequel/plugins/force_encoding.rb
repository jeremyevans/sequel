if RUBY_VERSION >= '1.9.0'
module Sequel
  module Plugins
    # The ForceEncoding plugin allows you force specific encodings for all
    # strings that are used by the model.  When model instances are loaded
    # from the database, all values in the hash that are strings are
    # forced to the given encoding.  Whenever you update a model column
    # attribute, the resulting value is forced to a given encoding if the
    # value is a string.  There are two ways to specify the encoding.  You
    # can either do so in the plugin call itself, or via the
    # forced_encoding class accessor.
    #
    # Usage:
    #
    #   # Force all strings to be UTF8 encoded in a all model subclasses
    #   # (called before loading subclasses)
    #   Sequel::Model.plugin :force_encoding, 'UTF-8'
    #
    #   # Force the encoding for the Album model to UTF8
    #   Album.plugin :force_encoding
    #   Album.forced_encoding = 'UTF-8'
    module ForceEncoding
      # Set the forced_encoding based on the value given in the plugin call.
      # Note that if a the plugin has been previously loaded, any previous
      # forced encoding is overruled, even if no encoding is given when calling
      # the plugin.
      def self.configure(model, encoding=nil)
        model.forced_encoding = encoding
      end

      module ClassMethods
        # The string encoding to force on a column string values
        attr_accessor :forced_encoding

        # Copy the forced_encoding value into the subclass
        def inherited(subclass)
          super
          subclass.forced_encoding = forced_encoding
        end
      end

      module InstanceMethods
        # Allow the force encoding plugin to work with the identity_map
        # plugin by typecasting new values.
        def merge_db_update(row)
          super(force_hash_encoding(row))
        end

        # Force the encoding of all string values when setting the instance's values.
        def set_values(row)
          super(force_hash_encoding(row))
        end

        private

        # Force the encoding for all string values in the given row hash.
        def force_hash_encoding(row)
          fe = model.forced_encoding
          row.values.each{|v| v.force_encoding(fe) if v.is_a?(String)} if fe
          row
        end

        # Force the encoding of all returned strings to the model's forced_encoding.
        def typecast_value(column, value)
          s = super
          s.force_encoding(model.forced_encoding) if s.is_a?(String) && model.forced_encoding
          s
        end
      end
    end
  end
end
else
  raise LoadError, 'ForceEncoding plugin only works on Ruby 1.9+'
end
