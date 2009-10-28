module Sequel
  module Plugins
    # The validation_helpers plugin contains instance method equivalents for most of the legacy
    # class-level validations.  The names and APIs are different, though. Example:
    #
    #   class Album < Sequel::Model
    #     plugin :validation_helpers
    #     def validate
    #       validates_min_length 1, :num_tracks
    #     end
    #   end
    #
    # The validates_unique validation has a unique API, but the other validations have
    # the API explained here:
    #
    # Arguments:
    # * atts - Single attribute symbol or an array of attribute symbols specifying the
    #   attribute(s) to validate.
    # Options:
    # * :allow_blank - Whether to skip the validation if the value is blank.  You should
    #   make sure all objects respond to blank if you use this option, which you can do by
    #   requiring 'sequel/extensions/blank'
    # * :allow_missing - Whether to skip the validation if the attribute isn't a key in the
    #   values hash.  This is different from allow_nil, because Sequel only sends the attributes
    #   in the values when doing an insert or update.  If the attribute is not present, Sequel
    #   doesn't specify it, so the database will use the table's default value.  This is different
    #   from having an attribute in values with a value of nil, which Sequel will send as NULL.
    #   If your database table has a non NULL default, this may be a good option to use.  You
    #   don't want to use allow_nil, because if the attribute is in values but has a value nil,
    #   Sequel will attempt to insert a NULL value into the database, instead of using the
    #   database's default.
    # * :allow_nil - Whether to skip the validation if the value is nil.
    # * :message - The message to use.  Can be a string which is used directly, or a
    #   proc which is called.  If the validation method takes a argument before the array of attributes,
    #   that argument is passed as an argument to the proc.  The exception is the
    #   validates_not_string method, which doesn't take an argument, but passes
    #   the schema type symbol as the argument to the proc.
    #
    # The default validation options for all models can be modified by
    # changing the values of the Sequel::Plugins::ValidationHelpers::DEFAULT_OPTIONS hash.  You
    # change change the default options on a per model basis
    # by overriding a private instance method default_validation_helpers_options.
    module ValidationHelpers
      # Default validation options used by Sequel.  Can be modified to change the error
      # messages for all models (e.g. for internationalization), or to set certain
      # default options for validations (e.g. :allow_nil=>true for all validates_format).
      DEFAULT_OPTIONS = {
        :exact_length=>{:message=>lambda{|exact| "is not #{exact} characters"}},
        :format=>{:message=>lambda{|with| 'is invalid'}},
        :includes=>{:message=>lambda{|set| "is not in range or set: #{set.inspect}"}},
        :integer=>{:message=>lambda{"is not a number"}},
        :length_range=>{:message=>lambda{|range| "is too short or too long"}},
        :max_length=>{:message=>lambda{|max| "is longer than #{max} characters"}},
        :min_length=>{:message=>lambda{|min| "is shorter than #{min} characters"}},
        :not_string=>{:message=>lambda{|type| type ? "is not a valid #{type}" : "is a string"}},
        :numeric=>{:message=>lambda{"is not a number"}},
        :presence=>{:message=>lambda{"is not present"}},
        :unique=>{:message=>lambda{'is already taken'}}
      }
      
      module InstanceMethods 
        # Check that the attribute values are the given exact length.
        def validates_exact_length(exact, atts, opts={})
          validatable_attributes_for_type(:exact_length, atts, opts){|a,v,m| validation_error_message(m, exact) unless v && v.length == exact}
        end

        # Check the string representation of the attribute value(s) against the regular expression with.
        def validates_format(with, atts, opts={})
          validatable_attributes_for_type(:format, atts, opts){|a,v,m| validation_error_message(m, with) unless v.to_s =~ with}
        end
    
        # Check attribute value(s) is included in the given set.
        def validates_includes(set, atts, opts={})
          validatable_attributes_for_type(:includes, atts, opts){|a,v,m| validation_error_message(m, set) unless set.include?(v)}
        end
    
        # Check attribute value(s) string representation is a valid integer.
        def validates_integer(atts, opts={})
          validatable_attributes_for_type(:integer, atts, opts) do |a,v,m|
            begin
              Kernel.Integer(v.to_s)
              nil
            rescue
              validation_error_message(m)
            end
          end
        end

        # Check that the attribute values length is in the specified range.
        def validates_length_range(range, atts, opts={})
          validatable_attributes_for_type(:length_range, atts, opts){|a,v,m| validation_error_message(m, range) unless v && range.include?(v.length)}
        end
    
        # Check that the attribute values are not longer than the given max length.
        def validates_max_length(max, atts, opts={})
          validatable_attributes_for_type(:max_length, atts, opts){|a,v,m| validation_error_message(m, max) unless v && v.length <= max}
        end

        # Check that the attribute values are not shorter than the given min length.
        def validates_min_length(min, atts, opts={})
          validatable_attributes_for_type(:min_length, atts, opts){|a,v,m| validation_error_message(m, min) unless v && v.length >= min}
        end

        # Check that the attribute value(s) is not a string.  This is generally useful
        # in conjunction with raise_on_typecast_failure = false, where you are
        # passing in string values for non-string attributes (such as numbers and dates).
        # If typecasting fails (invalid number or date), the value of the attribute will
        # be a string in an invalid format, and if typecasting succeeds, the value will
        # not be a string.
        def validates_not_string(atts, opts={})
          validatable_attributes_for_type(:not_string, atts, opts){|a,v,m| validation_error_message(m, (db_schema[a]||{})[:type]) if v.is_a?(String)}
        end
    
        # Check attribute value(s) string representation is a valid float.
        def validates_numeric(atts, opts={})
          validatable_attributes_for_type(:numeric, atts, opts) do |a,v,m|
            begin
              Kernel.Float(v.to_s)
              nil
            rescue
              validation_error_message(m)
            end
          end
        end
    
        # Check attribute value(s) is not considered blank by the database, but allow false values.
        def validates_presence(atts, opts={})
          validatable_attributes_for_type(:presence, atts, opts){|a,v,m| validation_error_message(m) if model.db.send(:blank_object?, v) && v != false}
        end
        
        # Checks that there are no duplicate values in the database for the given
        # attributes.  Pass an array of fields instead of multiple
        # fields to specify that the combination of fields must be unique,
        # instead of that each field should have a unique value.
        #
        # This means that the code:
        #   validates_unique([:column1, :column2])
        # validates the grouping of column1 and column2 while
        #   validates_unique(:column1, :column2)
        # validates them separately.
        #
        # You can pass a block, which is yielded the dataset in which the columns
        # must be unique. So if you are doing a soft delete of records, in which
        # the name must be unique, but only for active records:
        #
        #   validates_unique(:name){|ds| ds.filter(:active)}
        #
        # You should also add a unique index in the
        # database, as this suffers from a fairly obvious race condition.
        #
        # This validation does not respect the :allow_* options that the other validations accept,
        # since it can deal with a grouping of multiple attributes.
        #
        # Possible Options:
        # * :message - The message to use (default: 'is already taken')
        def validates_unique(*atts)
          opts = default_validation_helpers_options(:unique)
          if atts.last.is_a?(Hash)
            opts = opts.merge(atts.pop)
          end
          message = validation_error_message(opts[:message])
          atts.each do |a|
            ds = model.filter(Array(a).map{|x| [x, send(x)]})
            ds = yield(ds) if block_given?
            errors.add(a, message) unless (new? ? ds : ds.exclude(pk_hash)).count == 0
          end
        end
        
        private
        
        # The default options hash for the given type of validation.  Can
        # be overridden on a per-model basis for different per model defaults.
        # The hash return must include a :message option that is either a
        # proc or string.
        def default_validation_helpers_options(type)
          DEFAULT_OPTIONS[type]
        end

        # Skip validating any attribute that matches one of the allow_* options.
        # Otherwise, yield the attribute, value, and passed option :message to
        # the block.  If the block returns anything except nil or false, add it as
        # an error message for that attributes.
        def validatable_attributes(atts, opts)
          am, an, ab, m = opts.values_at(:allow_missing, :allow_nil, :allow_blank, :message)
          Array(atts).each do |a|
            next if am && !values.has_key?(a)
            v = send(a)
            next if an && v.nil?
            next if ab && v.respond_to?(:blank?) && v.blank?
            if message = yield(a, v, m)
              errors.add(a, message)
            end
          end
        end
        
        # Merge the given options with the default options for the given type
        # and call validatable_attributes with the merged options.
        def validatable_attributes_for_type(type, atts, opts, &block)
          validatable_attributes(atts, default_validation_helpers_options(type).merge(opts), &block)
        end
        
        # The validation error message to use, as a string.  If message
        # is a Proc, call it with the args.  Otherwise, assume it is a string and
        # return it.
        def validation_error_message(message, *args)
          message.is_a?(Proc) ? message.call(*args) : message
        end
      end
    end
  end
end
