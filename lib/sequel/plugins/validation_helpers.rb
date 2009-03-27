module Sequel
  module Plugins
    module ValidationHelpers
      # ValidationHelpers contains instance method equivalents for most of the previous
      # default validations.  The names and APIs have changed, though.  
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
      # * :message - The message to use
      module InstanceMethods 
        # Check that the attribute values are the given exact length.
        def validates_exact_length(exact, atts, opts={})
          validatable_attributes(atts, opts){|a,v,m| (m || "is not #{exact} characters") unless v && v.length == exact}
        end

        # Check the string representation of the attribute value(s) against the regular expression with.
        def validates_format(with, atts, opts={})
          validatable_attributes(atts, opts){|a,v,m| (m || 'is invalid') unless v.to_s =~ with}
        end
    
        # Check attribute value(s) is included in the given set.
        def validates_includes(set, atts, opts={})
          validatable_attributes(atts, opts){|a,v,m| (m || "is not in range or set: #{set.inspect}") unless set.include?(v)}
        end
    
        # Check attribute value(s) string representation is a valid integer.
        def validates_integer(atts, opts={})
          validatable_attributes(atts, opts) do |a,v,m|
            begin
              Kernel.Integer(v.to_s)
              nil
            rescue
              m || 'is not a number'
            end
          end
        end

        # Check that the attribute values length is in the specified range.
        def validates_length_range(range, atts, opts={})
          validatable_attributes(atts, opts){|a,v,m| (m || "is outside the allowed range") unless v && range.include?(v.length)}
        end
    
        # Check that the attribute values are not longer than the given max length.
        def validates_max_length(max, atts, opts={})
          validatable_attributes(atts, opts){|a,v,m| (m || "is longer than #{max} characters") unless v && v.length <= max}
        end

        # Check that the attribute values are not shorter than the given min length.
        def validates_min_length(min, atts, opts={})
          validatable_attributes(atts, opts){|a,v,m| (m || "is shorter than #{min} characters") unless v && v.length >= min}
        end

        # Check that the attribute value(s) is not a string.  This is generally useful
        # in conjunction with raise_on_typecast_failure = false, where you are
        # passing in string values for non-string attributes (such as numbers and dates).
        # If typecasting fails (invalid number or date), the value of the attribute will
        # be a string in an invalid format, and if typecasting succeeds, the value will
        # not be a string.
        def validates_not_string(atts, opts={})
          validatable_attributes(atts, opts) do |a,v,m|
            next unless v.is_a?(String)
            next m if m
            (sch = db_schema[a] and typ = sch[:type]) ?  "is not a valid #{typ}" : "is a string"
          end
        end
    
        # Check attribute value(s) string representation is a valid float.
        def validates_numeric(atts, opts={})
          validatable_attributes(atts, opts) do |a,v,m|
            begin
              Kernel.Float(v.to_s)
              nil
            rescue
              m || 'is not a number'
            end
          end
        end
    
        # Check attribute value(s) is not considered blank by the database, but allow false values.
        def validates_presence(atts, opts={})
          validatable_attributes(atts, opts){|a,v,m| (m || "is not present") if model.db.send(:blank_object?, v) && v != false}
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
        # You should also add a unique index in the
        # database, as this suffers from a fairly obvious race condition.
        #
        # This validation does not respect the :allow_* options that the other validations accept,
        # since it can deals with multiple attributes at once.
        #
        # Possible Options:
        # * :message - The message to use (default: 'is already taken')
        def validates_unique(*atts)
          message = (atts.pop[:message] if atts.last.is_a?(Hash)) || 'is already taken'
          atts.each do |a|
            ds = model.filter(Array(a).map{|x| [x, send(x)]})
            errors[a] << message unless (new? ? ds : ds.exclude(pk_hash)).count == 0
          end
        end
        
        private

        # Skip validating any attribute that matches one of the allow_* options.
        # Otherwise, yield the attribute, value, and passed option :message to
        # the block.  If the block returns anything except nil or false, add it as
        # an error message for that attributes.
        def validatable_attributes(atts, opts)
          Array(atts).each do |a|
            next if opts[:allow_missing] && !values.has_key?(a)
            v = send(a)
            next if opts[:allow_nil] && value.nil?
            next if opts[:allow_blank] && value.respond_to?(:blank?) && value.blank?
            if message = yield(a, v, opts[:message])
              errors[a] << message
            end
          end
        end
      end
    end
  end
end
