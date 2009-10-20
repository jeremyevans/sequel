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
    #   proc which is called.
    #
    # The default validation error messages for all models can be modified by
    # changing the values of the ValidationHelpers::MESSAGE_PROCS hash.  You
    # change change the default validation error messages on a per model basis
    # by overriding a private instance method named default_validation_error_message_proc.
    module ValidationHelpers
      # Default validation message procs used by Sequel.  Can be modified to change the error
      # messages for all models, or for internationalization.  Uses procs instead of format
      # strings to allow for complete flexibility.
      #
      # If the validation method takes a argument before the array of attributes,
      # that argument is passed as an argument to the proc.  The exception is the
      # validates_not_string method, which doesn't take an argument, but passes
      # the schema type symbol as the argument to the proc.
      MESSAGE_PROCS = {
        :exact_length=>lambda{|exact| "is not #{exact} characters"},
        :format=>lambda{|with| 'is invalid'},
        :includes=>lambda{|set| "is not in range or set: #{set.inspect}"},
        :integer=>lambda{"is not a number"},
        :length_range=>lambda{|range| "is too short or too long"},
        :max_length=>lambda{|max| "is longer than #{max} characters"},
        :min_length=>lambda{|min| "is shorter than #{min} characters"},
        :not_string=>lambda{|type| type ? "is not a valid #{type}" : "is a string"},
        :numeric=>lambda{"is not a number"},
        :presence=>lambda{"is not present"},
        :unique=>lambda{'is already taken'}
      }
      
      module InstanceMethods 
        # Check that the attribute values are the given exact length.
        def validates_exact_length(exact, atts, opts={})
          validatable_attributes(atts, opts){|a,v,m| validation_error_message(m, :exact_length, exact) unless v && v.length == exact}
        end

        # Check the string representation of the attribute value(s) against the regular expression with.
        def validates_format(with, atts, opts={})
          validatable_attributes(atts, opts){|a,v,m| validation_error_message(m, :format, with) unless v.to_s =~ with}
        end
    
        # Check attribute value(s) is included in the given set.
        def validates_includes(set, atts, opts={})
          validatable_attributes(atts, opts){|a,v,m| validation_error_message(m, :includes, set) unless set.include?(v)}
        end
    
        # Check attribute value(s) string representation is a valid integer.
        def validates_integer(atts, opts={})
          validatable_attributes(atts, opts) do |a,v,m|
            begin
              Kernel.Integer(v.to_s)
              nil
            rescue
              validation_error_message(m, :integer)
            end
          end
        end

        # Check that the attribute values length is in the specified range.
        def validates_length_range(range, atts, opts={})
          validatable_attributes(atts, opts){|a,v,m| validation_error_message(m, :length_range, range) unless v && range.include?(v.length)}
        end
    
        # Check that the attribute values are not longer than the given max length.
        def validates_max_length(max, atts, opts={})
          validatable_attributes(atts, opts){|a,v,m| validation_error_message(m, :max_length, max) unless v && v.length <= max}
        end

        # Check that the attribute values are not shorter than the given min length.
        def validates_min_length(min, atts, opts={})
          validatable_attributes(atts, opts){|a,v,m| validation_error_message(m, :min_length, min) unless v && v.length >= min}
        end

        # Check that the attribute value(s) is not a string.  This is generally useful
        # in conjunction with raise_on_typecast_failure = false, where you are
        # passing in string values for non-string attributes (such as numbers and dates).
        # If typecasting fails (invalid number or date), the value of the attribute will
        # be a string in an invalid format, and if typecasting succeeds, the value will
        # not be a string.
        def validates_not_string(atts, opts={})
          validatable_attributes(atts, opts){|a,v,m| validation_error_message(m, :not_string, (db_schema[a]||{})[:type]) if v.is_a?(String)}
        end
    
        # Check attribute value(s) string representation is a valid float.
        def validates_numeric(atts, opts={})
          validatable_attributes(atts, opts) do |a,v,m|
            begin
              Kernel.Float(v.to_s)
              nil
            rescue
              validation_error_message(m, :numeric)
            end
          end
        end
    
        # Check attribute value(s) is not considered blank by the database, but allow false values.
        def validates_presence(atts, opts={})
          validatable_attributes(atts, opts){|a,v,m| validation_error_message(m, :presence) if model.db.send(:blank_object?, v) && v != false}
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
          message = validation_error_message((atts.pop[:message] if atts.last.is_a?(Hash)), :unique)
          atts.each do |a|
            ds = model.filter(Array(a).map{|x| [x, send(x)]})
            ds = yield(ds) if block_given?
            errors.add(a, message) unless (new? ? ds : ds.exclude(pk_hash)).count == 0
          end
        end
        
        private
        
        # A default proc for the given type that can be called to produce a
        # validation error message.  Can be overridden on a per model basis.
        def default_validation_error_message_proc(type)
          MESSAGE_PROCS[type]
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
        
        # The validation error message to use, as a string.  If an override_message
        # argument is nil, call the default proc with the args.  Otherwise, if it
        # is a proc, call it with the args.  Otherwise, assume it is a string and
        # return it.
        def validation_error_message(override_message, type, *args)
          if override_message
            override_message.is_a?(Proc) ? override_message.call(*args) : override_message
          else
            default_validation_error_message_proc(type).call(*args)
          end
        end
      end
    end
  end
end
