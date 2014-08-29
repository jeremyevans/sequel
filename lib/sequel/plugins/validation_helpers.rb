module Sequel
  module Plugins
    # The validation_helpers plugin contains instance method equivalents for most of the legacy
    # class-level validations.  The names and APIs are different, though. Example:
    #
    #   Sequel::Model.plugin :validation_helpers
    #   class Album < Sequel::Model
    #     def validate
    #       super
    #       validates_min_length 1, :num_tracks
    #     end
    #   end
    #
    # The validates_unique method has a unique API, but the other validations have the API explained here:
    #
    # Arguments:
    # atts :: Single attribute symbol or an array of attribute symbols specifying the
    #         attribute(s) to validate.
    # Options:
    # :allow_blank :: Whether to skip the validation if the value is blank.  You should
    #                 make sure all objects respond to blank if you use this option, which you can do by:
    #                     Sequel.extension :blank
    # :allow_missing :: Whether to skip the validation if the attribute isn't a key in the
    #                   values hash.  This is different from allow_nil, because Sequel only sends the attributes
    #                   in the values when doing an insert or update.  If the attribute is not present, Sequel
    #                   doesn't specify it, so the database will use the table's default value.  This is different
    #                   from having an attribute in values with a value of nil, which Sequel will send as NULL.
    #                   If your database table has a non NULL default, this may be a good option to use.  You
    #                   don't want to use allow_nil, because if the attribute is in values but has a value nil,
    #                   Sequel will attempt to insert a NULL value into the database, instead of using the
    #                   database's default.
    # :allow_nil :: Whether to skip the validation if the value is nil.
    # :message :: The message to use.  Can be a string which is used directly, or a
    #             proc which is called.  If the validation method takes a argument before the array of attributes,
    #             that argument is passed as an argument to the proc.
    #
    # The default validation options for all models can be modified by
    # changing the values of the Sequel::Plugins::ValidationHelpers::DEFAULT_OPTIONS hash.  You
    # change change the default options on a per model basis
    # by overriding a private instance method default_validation_helpers_options.
    #
    # By changing the default options, you can setup internationalization of the
    # error messages.  For example, you would modify the default options:
    #
    #   Sequel::Plugins::ValidationHelpers::DEFAULT_OPTIONS.merge!(
    #     :exact_length=>{:message=>lambda{|exact| I18n.t("errors.exact_length", :exact => exact)}},
    #     :integer=>{:message=>lambda{I18n.t("errors.integer")}}
    #   )
    #
    # and then use something like this in your yaml translation file:
    #
    #   en:
    #     errors:
    #       exact_length: "is not %{exact} characters"
    #       integer: "is not a number"
    #
    # Note that if you want to support internationalization of Errors#full_messages,
    # you need to override the method.  Here's an example:
    #   
    #   class Sequel::Model::Errors
    #     ATTRIBUTE_JOINER = I18n.t('errors.joiner').freeze
    #     def full_messages
    #       inject([]) do |m, kv|
    #         att, errors = *kv
    #         att.is_a?(Array) ? Array(att).map!{|v| I18n.t("attributes.#{v}")} : att = I18n.t("attributes.#{att}")
    #         errors.each {|e| m << (e.is_a?(LiteralString) ? e : "#{Array(att).join(ATTRIBUTE_JOINER)} #{e}")}
    #         m
    #       end
    #     end
    #   end
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
        :max_length=>{:message=>lambda{|max| "is longer than #{max} characters"}, :nil_message=>lambda{"is not present"}},
        :min_length=>{:message=>lambda{|min| "is shorter than #{min} characters"}},
        :not_null=>{:message=>lambda{"is not present"}},
        :numeric=>{:message=>lambda{"is not a number"}},
        :type=>{:message=>lambda{|klass| klass.is_a?(Array) ? "is not a valid #{klass.join(" or ").downcase}" : "is not a valid #{klass.to_s.downcase}"}},
        :presence=>{:message=>lambda{"is not present"}},
        :unique=>{:message=>lambda{'is already taken'}}
      }

      module InstanceMethods 
        # Check that the attribute values are the given exact length.
        def validates_exact_length(exact, atts, opts=OPTS)
          validatable_attributes_for_type(:exact_length, atts, opts){|a,v,m| validation_error_message(m, exact) if v.nil? || v.length != exact}
        end

        # Check the string representation of the attribute value(s) against the regular expression with.
        def validates_format(with, atts, opts=OPTS)
          validatable_attributes_for_type(:format, atts, opts){|a,v,m| validation_error_message(m, with) unless v.to_s =~ with}
        end
    
        # Check attribute value(s) is included in the given set.
        def validates_includes(set, atts, opts=OPTS)
          validatable_attributes_for_type(:includes, atts, opts){|a,v,m| validation_error_message(m, set) unless set.send(set.respond_to?(:cover?) ? :cover? : :include?, v)}
        end
    
        # Check attribute value(s) string representation is a valid integer.
        def validates_integer(atts, opts=OPTS)
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
        def validates_length_range(range, atts, opts=OPTS)
          validatable_attributes_for_type(:length_range, atts, opts){|a,v,m| validation_error_message(m, range) if v.nil? || !range.send(range.respond_to?(:cover?) ? :cover? : :include?, v.length)}
        end
    
        # Check that the attribute values are not longer than the given max length.
        #
        # Accepts a :nil_message option that is the error message to use when the
        # value is nil instead of being too long.
        def validates_max_length(max, atts, opts=OPTS)
          validatable_attributes_for_type(:max_length, atts, opts){|a,v,m| v ? validation_error_message(m, max) : validation_error_message(opts[:nil_message] || DEFAULT_OPTIONS[:max_length][:nil_message]) if v.nil? || v.length > max}
        end

        # Check that the attribute values are not shorter than the given min length.
        def validates_min_length(min, atts, opts=OPTS)
          validatable_attributes_for_type(:min_length, atts, opts){|a,v,m| validation_error_message(m, min) if v.nil? || v.length < min}
        end

        # Check attribute value(s) are not NULL/nil.
        def validates_not_null(atts, opts=OPTS)
          validatable_attributes_for_type(:not_null, atts, opts){|a,v,m| validation_error_message(m) if v.nil?}
        end
        
        # Check attribute value(s) string representation is a valid float.
        def validates_numeric(atts, opts=OPTS)
          validatable_attributes_for_type(:numeric, atts, opts) do |a,v,m|
            begin
              Kernel.Float(v.to_s)
              nil
            rescue
              validation_error_message(m)
            end
          end
        end

        # Validates for all of the model columns (or just the given columns)
        # that the column value is an instance of the expected class based on
        # the column's schema type.
        def validates_schema_types(atts=keys, opts=OPTS)
          Array(atts).each do |k|
            if type = schema_type_class(k)
              validates_type(type, k, {:allow_nil=>true}.merge(opts))
            end
          end
        end

        # Check if value is an instance of a class.  If +klass+ is an array,
        # the value must be an instance of one of the classes in the array.
        def validates_type(klass, atts, opts=OPTS)
          klass = klass.to_s.constantize if klass.is_a?(String) || klass.is_a?(Symbol)
          validatable_attributes_for_type(:type, atts, opts) do |a,v,m|
            if klass.is_a?(Array) ? !klass.any?{|kls| v.is_a?(kls)} : !v.is_a?(klass)
              validation_error_message(m, klass)
            end
          end
        end

        # Check attribute value(s) is not considered blank by the database, but allow false values.
        def validates_presence(atts, opts=OPTS)
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
        # :dataset :: The base dataset to use for the unique query, defaults to the
        #             model's dataset.
        # :message :: The message to use (default: 'is already taken')
        # :only_if_modified :: Only check the uniqueness if the object is new or
        #                      one of the columns has been modified.
        # :where :: A callable object where call takes three arguments, a dataset,
        #           the current object, and an array of columns, and should return
        #           a modified dataset that is filtered to include only rows with
        #           the same values as the current object for each column in the array.
        #
        # If you want to to a case insensitive uniqueness validation on a database that
        # is case sensitive by default, you can use:
        #
        #   validates_unique :column, :where=>(proc do |ds, obj, cols|
        #     ds.where(cols.map do |c|
        #       v = obj.send(c)
        #       v = v.downcase if v
        #       [Sequel.function(:lower, c), v]
        #     end)
        #   end)
        def validates_unique(*atts)
          opts = default_validation_helpers_options(:unique)
          if atts.last.is_a?(Hash)
            opts = opts.merge(atts.pop)
          end
          message = validation_error_message(opts[:message])
          where = opts[:where]
          atts.each do |a|
            arr = Array(a)
            next if arr.any?{|x| errors.on(x)}
            next if opts[:only_if_modified] && !new? && !arr.any?{|x| changed_columns.include?(x)}
            ds = opts[:dataset] || model.dataset
            ds = if where
              where.call(ds, self, arr)
            else
              vals = arr.map{|x| send(x)}
              next if vals.any?{|v| v.nil?}
              ds.where(arr.zip(vals))
            end
            ds = yield(ds) if block_given?
            ds = ds.exclude(pk_hash) unless new?
            errors.add(a, message) unless ds.count == 0
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
