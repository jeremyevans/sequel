module Sequel
  module Plugins
    module DeprecatedValidationClassMethods
      module ClassMethods
        # The Generator class is used to generate validation definitions using 
        # the validates {} idiom.
        class Generator
          # Initializes a new generator.
          def initialize(receiver ,&block)
            @receiver = receiver
            instance_eval(&block)
          end
      
          # Delegates method calls to the receiver by calling receiver.validates_xxx.
          def method_missing(m, *args, &block)
            @receiver.send(:"validates_#{m}", *args, &block)
          end
        end
    
        # Returns true if validations are defined.
        def has_validations?
          !validations.empty?
        end
    
        # Instructs the model to skip validations defined in superclasses
        def skip_superclass_validations
          @skip_superclass_validations = true
        end
        
        # Defines validations by converting a longhand block into a series of 
        # shorthand definitions. For example:
        #
        #   class MyClass < Sequel::Model
        #     validates do
        #       length_of :name, :minimum => 6
        #       length_of :password, :minimum => 8
        #     end
        #   end
        #
        # is equivalent to:
        #   class MyClass < Sequel::Model
        #     validates_length_of :name, :minimum => 6
        #     validates_length_of :password, :minimum => 8
        #   end
        def validates(&block)
          Deprecation.deprecate('Sequel::Model.validates', 'Use Model.plugin(:validation_class_methods) first')
          Generator.new(self, &block)
        end
    
        # Validates the given instance.
        def validate(o)
          if superclass.respond_to?(:validate) && !@skip_superclass_validations
            superclass.validate(o)
          end
          validations.each do |att, procs|
            v = case att
            when Array
              att.collect{|a| o.send(a)}
            else
              o.send(att)
            end
            procs.each {|tag, p| p.call(o, att, v)}
          end
        end
        
        # Validates acceptance of an attribute.  Just checks that the value
        # is equal to the :accept option. This method is unique in that
        # :allow_nil is assumed to be true instead of false.
        #
        # Possible Options:
        # * :accept - The value required for the object to be valid (default: '1')
        # * :message - The message to use (default: 'is not accepted')
        def validates_acceptance_of(*atts)
          Deprecation.deprecate('Sequel::Model.validates_acceptance_of', 'Use Model.plugin(:validation_class_methods) first')
          opts = {
            :message => 'is not accepted',
            :allow_nil => true,
            :accept => '1',
            :tag => :acceptance,
          }.merge!(extract_options!(atts))
          atts << opts
          validates_each(*atts) do |o, a, v|
            o.errors[a] << opts[:message] unless v == opts[:accept]
          end
        end
    
        # Validates confirmation of an attribute. Checks that the object has
        # a _confirmation value matching the current value.  For example:
        #
        #   validates_confirmation_of :blah
        #
        # Just makes sure that object.blah = object.blah_confirmation.  Often used for passwords
        # or email addresses on web forms.
        #
        # Possible Options:
        # * :message - The message to use (default: 'is not confirmed')
        def validates_confirmation_of(*atts)
          Deprecation.deprecate('Sequel::Model.validates_confirmation_of', 'Use Model.plugin(:validation_class_methods) first')
          opts = {
            :message => 'is not confirmed',
            :tag => :confirmation,
          }.merge!(extract_options!(atts))
          atts << opts
          validates_each(*atts) do |o, a, v|
            o.errors[a] << opts[:message] unless v == o.send(:"#{a}_confirmation")
          end
        end
    
        # Adds a validation for each of the given attributes using the supplied
        # block. The block must accept three arguments: instance, attribute and 
        # value, e.g.:
        #
        #   validates_each :name, :password do |object, attribute, value|
        #     object.errors[attribute] << 'is not nice' unless value.nice?
        #   end
        #
        # Possible Options:
        # * :allow_blank - Whether to skip the validation if the value is blank. 
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
        # * :if - A symbol (indicating an instance_method) or proc (which is instance_evaled)
        #   skipping this validation if it returns nil or false.
        # * :tag - The tag to use for this validation.
        def validates_each(*atts, &block)
          Deprecation.deprecate('Sequel::Model.validates_each', 'Use Model.plugin(:validation_class_methods) first')
          opts = extract_options!(atts)
          blk = if (i = opts[:if]) || (am = opts[:allow_missing]) || (an = opts[:allow_nil]) || (ab = opts[:allow_blank])
            proc do |o,a,v|
              next if i && !validation_if_proc(o, i)
              next if an && Array(v).all?{|x| x.nil?}
              next if ab && Array(v).all?{|x| x.blank?}
              next if am && Array(a).all?{|x| !o.values.has_key?(x)}
              block.call(o,a,v)
            end
          else
            block
          end
          tag = opts[:tag]
          atts.each do |a| 
            a_vals = validations[a]
            if tag && (old = a_vals.find{|x| x[0] == tag})
              old[1] = blk
            else
              a_vals << [tag, blk]
            end
          end
        end
    
        # Validates the format of an attribute, checking the string representation of the
        # value against the regular expression provided by the :with option.
        #
        # Possible Options:
        # * :message - The message to use (default: 'is invalid')
        # * :with - The regular expression to validate the value with (required).
        def validates_format_of(*atts)
          Deprecation.deprecate('Sequel::Model.validates_format_of', 'Use Model.plugin(:validation_class_methods) first')
          opts = {
            :message => 'is invalid',
            :tag => :format,
          }.merge!(extract_options!(atts))
          
          unless opts[:with].is_a?(Regexp)
            raise ArgumentError, "A regular expression must be supplied as the :with option of the options hash"
          end
          
          atts << opts
          validates_each(*atts) do |o, a, v|
            o.errors[a] << opts[:message] unless v.to_s =~ opts[:with]
          end
        end
    
        # Validates the length of an attribute.
        #
        # Possible Options:
        # * :is - The exact size required for the value to be valid (no default)
        # * :maximum - The maximum size allowed for the value (no default)
        # * :message - The message to use (no default, overrides :too_long, :too_short, and :wrong_length
        #   options if present)
        # * :minimum - The minimum size allowed for the value (no default)
        # * :too_long - The message to use use if it the value is too long (default: 'is too long')
        # * :too_short - The message to use use if it the value is too short (default: 'is too short')
        # * :within - The array/range that must include the size of the value for it to be valid (no default)
        # * :wrong_length - The message to use use if it the value is not valid (default: 'is the wrong length')
        def validates_length_of(*atts)
          Deprecation.deprecate('Sequel::Model.validates_length_of', 'Use Model.plugin(:validation_class_methods) first')
          opts = {
            :too_long     => 'is too long',
            :too_short    => 'is too short',
            :wrong_length => 'is the wrong length'
          }.merge!(extract_options!(atts))
          
          opts[:tag] ||= ([:length] + [:maximum, :minimum, :is, :within].reject{|x| !opts.include?(x)}).join('-').to_sym
          atts << opts
          validates_each(*atts) do |o, a, v|
            if m = opts[:maximum]
              o.errors[a] << (opts[:message] || opts[:too_long]) unless v && v.size <= m
            end
            if m = opts[:minimum]
              o.errors[a] << (opts[:message] || opts[:too_short]) unless v && v.size >= m
            end
            if i = opts[:is]
              o.errors[a] << (opts[:message] || opts[:wrong_length]) unless v && v.size == i
            end
            if w = opts[:within]
              o.errors[a] << (opts[:message] || opts[:wrong_length]) unless v && w.include?(v.size)
            end
          end
        end
    
        # Validates whether an attribute is not a string.  This is generally useful
        # in conjunction with raise_on_typecast_failure = false, where you are
        # passing in string values for non-string attributes (such as numbers and dates).
        # If typecasting fails (invalid number or date), the value of the attribute will
        # be a string in an invalid format, and if typecasting succeeds, the value will
        # not be a string.
        #
        # Possible Options:
        # * :message - The message to use (default: 'is a string' or 'is not a valid (integer|datetime|etc.)' if the type is known)
        def validates_not_string(*atts)
          Deprecation.deprecate('Sequel::Model.validates_not_string', 'Use Model.plugin(:validation_class_methods) first')
          opts = {
            :tag => :not_string,
          }.merge!(extract_options!(atts))
          atts << opts
          validates_each(*atts) do |o, a, v|
            if v.is_a?(String)
              unless message = opts[:message]
                message = if sch = o.db_schema[a] and typ = sch[:type]
                  "is not a valid #{typ}"
                else
                  "is a string"
                end
              end
              o.errors[a] << message
            end
          end
        end
    
        # Validates whether an attribute is a number.
        #
        # Possible Options:
        # * :message - The message to use (default: 'is not a number')
        # * :only_integer - Whether only integers are valid values (default: false)
        def validates_numericality_of(*atts)
          Deprecation.deprecate('Sequel::Model.validates_numericality_of', 'Use Model.plugin(:validation_class_methods) first')
          opts = {
            :message => 'is not a number',
            :tag => :numericality,
          }.merge!(extract_options!(atts))
          atts << opts
          validates_each(*atts) do |o, a, v|
            begin
              if opts[:only_integer]
                Kernel.Integer(v.to_s)
              else
                Kernel.Float(v.to_s)
              end
            rescue
              o.errors[a] << opts[:message]
            end
          end
        end
    
        # Validates the presence of an attribute.  Requires the value not be blank,
        # with false considered present instead of absent.
        #
        # Possible Options:
        # * :message - The message to use (default: 'is not present')
        def validates_presence_of(*atts)
          Deprecation.deprecate('Sequel::Model.validates_presence_of', 'Use Model.plugin(:validation_class_methods) first')
          opts = {
            :message => 'is not present',
            :tag => :presence,
          }.merge!(extract_options!(atts))
          atts << opts
          validates_each(*atts) do |o, a, v|
            o.errors[a] << opts[:message] if v.blank? && v != false
          end
        end
        
        # Validates that an attribute is within a specified range or set of values.
        #
        # Possible Options:
        # * :in - An array or range of values to check for validity (required)
        # * :message - The message to use (default: 'is not in range or set: <specified range>')
        def validates_inclusion_of(*atts)
          Deprecation.deprecate('Sequel::Model.validates_inclusion_of', 'Use Model.plugin(:validation_class_methods) first')
          opts = extract_options!(atts)
          unless opts[:in] && opts[:in].respond_to?(:include?) 
            raise ArgumentError, "The :in parameter is required, and respond to include?"
          end
          opts[:message] ||= "is not in range or set: #{opts[:in].inspect}"
          atts << opts
          validates_each(*atts) do |o, a, v|
            o.errors[a] << opts[:message] unless opts[:in].include?(v)
          end
        end
    
        # Validates only if the fields in the model (specified by atts) are
        # unique in the database.  Pass an array of fields instead of multiple
        # fields to specify that the combination of fields must be unique,
        # instead of that each field should have a unique value.
        #
        # This means that the code:
        #   validates_uniqueness_of([:column1, :column2])
        # validates the grouping of column1 and column2 while
        #   validates_uniqueness_of(:column1, :column2)
        # validates them separately.
        #
        # You should also add a unique index in the
        # database, as this suffers from a fairly obvious race condition.
        #
        # Possible Options:
        # * :message - The message to use (default: 'is already taken')
        def validates_uniqueness_of(*atts)
          Deprecation.deprecate('Sequel::Model.validates_uniqueness_of', 'Use Model.plugin(:validation_class_methods) first')
          opts = {
            :message => 'is already taken',
            :tag => :uniqueness,
          }.merge!(extract_options!(atts))
    
          atts << opts
          validates_each(*atts) do |o, a, v|
            error_field = a
            a = Array(a)
            v = Array(v)
            ds = o.class.filter(a.zip(v))
            num_dups = ds.count
            allow = if num_dups == 0
              # No unique value in the database
              true
            elsif num_dups > 1
              # Multiple "unique" values in the database!!
              # Someone didn't add a unique index
              false
            elsif o.new?
              # New record, but unique value already exists in the database
              false
            elsif ds.first === o
              # Unique value exists in database, but for the same record, so the update won't cause a duplicate record
              true
            else
              false
            end
            o.errors[error_field] << opts[:message] unless allow
          end
        end
    
        # Returns the validations hash for the class.
        def validations
          @validations ||= Hash.new {|h, k| h[k] = []}
        end
        
        private
    
        def extract_options!(array)
          array.last.is_a?(Hash) ? array.pop : {}
        end
  
        def validation_if_proc(o, i)
          case i
          when Symbol then o.send(i)
          when Proc then o.instance_eval(&i)
          when nil then true
          else raise(::Sequel::Error, "invalid value for :if validation option")
          end
        end
      end
    
      module InstanceMethods
        # Validates the object.
        def validate
          model.validate(self)
        end
      end
    end
  end

  Model.plugin :deprecated_validation_class_methods
end
