module Sequel
  class Model
    # =Basic Sequel Validations
    #
    # Sequel validations are based on the Validatable gem http://validatable.rubyforge.org/
    # 
    # To assign default validations to a sequel model:
    # 
    # class MyModel < SequelModel(:items)
    #   validates do
    #     format_of...
    #     presence_of...
    #     acceptance_of...
    #     confirmation_of...
    #     length_of...
    #     true_for...
    #     numericality_of...
    #     format_of...
    #     validates_base...
    #     validates_each...
    #   end
    # end
    #
    # You may also perform the usual 'longhand' way to assign default model validates 
    # directly within the model class itself:
    #
    #   class MyModel < SequelModel(:items)
    #     validates_format_of...
    #     validates_presence_of...
    #     validates_acceptance_of...
    #     validates_confirmation_of...
    #     validates_length_of...
    #     validates_true_for...
    #     validates_numericality_of...
    #     validates_format_of...
    #     validates_base...
    #     validates_each...
    #   end
    #
    # Each validation allows for arguments:
    #   TODO: fill the argument options in here
    #
    # =Advanced Sequel Validations
    #
    # TODO: verify that advanced validates work as stated (aka write specs)
    # NOTE: experimental
    #
    #  To store validates for conditional usage simply specify a name with which to store them
    #    class User < Sequel::Model
    #
    #      # This set of validates becomes stored as :default and gets injected into the model.
    #      validates do
    #        # standard validates calls
    #      end
    #      
    #      validates(:registration) do
    #        # user registration specific validates 
    #      end
    #      
    #      validates(:promotion) do
    #        # user promotion validates
    #      end
    #
    #    end
    #
    # To use the above validates:
    #
    #   @user.valid?                # Runs the default validations only.
    #   @user.valid?(:registration) # Runs both default and registration validations
    #   @user.valid?(:promotion)    # Runs both default and promotion validations
    #
    # You may determine whether the model has validates via:
    #
    #   validations? # will return true / false based on existence of validations on the model.
    #
    # You may also retrieve the validations block if needed:
    #
    #   validates(:registration) # returns the registration validation block.
    #
    # validates() method parameters:
    #   a validations block - runs the validations block on the model & stores as :default
    #   a name and a validations block - stores the block under the name
    #   a name - returns a stored block of that name or nil
    #   nothing - returns true / false based on if validations exist for the model.
    #
    module Validations
      class Generator
        def initialize(model_class ,&block)
          @model_class = model_class
          instance_eval(&block)
        end

        def method_missing(method, *args)
          method = :"validates_#{method}"
          @model_class.send(method, *args)
        end
      end
    end
    
    begin
      require "validatable"
      include ::Validatable
      def self.validates(&block)
        Validations::Generator.new(self, &block)
      end
      # return true if there are validations stored, false otherwise
      def self.has_validations?
        validations.length > 0 ? true : false
      end
    rescue LoadError
      STDERR.puts "Install the validatable gem in order to use Sequel Model validations"
      STDERR.puts "If you would like model validations to work, install the validatable gem."
      STDERR.puts "  (required for using the merb_helpers gem for example)"
    end
  end
end