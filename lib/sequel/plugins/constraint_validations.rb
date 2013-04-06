module Sequel
  module Plugins
    # The constraint_validations plugin is designed to be used with databases
    # that used the constraint_validations extension when creating their
    # tables.  The extension adds validation metadata for constraints created,
    # and this plugin reads that metadata and automatically creates validations
    # for all of the constraints.  For example, if you used the extension
    # and created your albums table like this:
    #
    #   DB.create_table(:albums) do
    #     primary_key :id
    #     String :name
    #     validate do
    #       min_length 5, :name
    #     end
    #   end
    #
    # Then when you went to save an album that uses this plugin:
    #
    #   Album.create(:name=>'abc')
    #   # raises Sequel::ValidationFailed: name is shorter than 5 characters
    # 
    # Usage:
    #
    #   # Make all model subclasses use constraint validations (called before loading subclasses)
    #   Sequel::Model.plugin :constraint_validations
    #
    #   # Make the Album class use constraint validations
    #   Album.plugin :constraint_validations
    module ConstraintValidations
      # The default constraint validation metadata table name.
      DEFAULT_CONSTRAINT_VALIDATIONS_TABLE = :sequel_constraint_validations

      # Automatically load the validation_helpers plugin to run the actual validations.
      def self.apply(model, opts={})
        model.plugin :validation_helpers
      end

      # Parse the constraint validations metadata from the database. Options:
      # :constraint_validations_table :: Override the name of the constraint validations
      #                                  metadata table.  Should only be used if the table
      #                                  name was overridden when creating the constraint
      #                                  validations.
      def self.configure(model, opts={})
        model.instance_variable_set(:@constraint_validations_table, opts[:constraint_validations_table] || DEFAULT_CONSTRAINT_VALIDATIONS_TABLE)
        model.send(:parse_constraint_validations)
      end

      module DatabaseMethods
        # A hash of validation method call metadata for all tables in the database.
        # The hash is keyed by table name string and contains arrays of validation
        # method call arrays.
        attr_accessor :constraint_validations
      end

      module ClassMethods
        # An array of validation method call arrays.  Each array is an array that
        # is splatted to send to perform a validation via validation_helpers.
        attr_reader :constraint_validations

        # The name of the table containing the constraint validations metadata.
        attr_reader :constraint_validations_table

        Plugins.inherited_instance_variables(self, :@constraint_validations_table=>nil)
        Plugins.after_set_dataset(self, :parse_constraint_validations)

        private

        # If the database has not already parsed constraint validation
        # metadata, then run a query to get the metadata data and transform it
        # into arrays of validation method calls.
        #
        # If this model has associated dataset, use the model's table name
        # to get the validations for just this model.
        def parse_constraint_validations
          db.extend(DatabaseMethods)

          unless hash = Sequel.synchronize{db.constraint_validations}
            hash = {}
            db.from(constraint_validations_table).each do |r|
              (hash[r[:table]] ||= []) << constraint_validation_array(r)
            end
            Sequel.synchronize{db.constraint_validations = hash}
          end

          if @dataset
            ds = @dataset.clone
            ds.quote_identifiers = false
            table_name = ds.literal(model.table_name)
            @constraint_validations = Sequel.synchronize{hash[table_name]} || []
          end
        end

        # Given a specific database constraint validation metadata row hash, transform
        # it in an validation method call array suitable for splatting to send.
        def constraint_validation_array(r)
          opts = {}
          opts[:message] = r[:message] if r[:message]
          opts[:allow_nil] = true if db.typecast_value(:boolean, r[:allow_nil])
          type = r[:validation_type].to_sym
          arg = r[:argument]
          column = r[:column]

          case type
          when :like, :ilike
            arg = constraint_validation_like_to_regexp(arg, type == :ilike)
            type = :format
          when :exact_length, :min_length, :max_length
            arg = arg.to_i
          when :length_range
            arg = constraint_validation_int_range(arg)
          when :format
            arg = Regexp.new(arg)
          when :iformat
            arg = Regexp.new(arg, Regexp::IGNORECASE)
            type = :format
          when :includes_str_array
            arg = arg.split(',')
            type = :includes
          when :includes_int_array
            arg = arg.split(',').map{|x| x.to_i}
            type = :includes
          when :includes_int_range
            arg = constraint_validation_int_range(arg)
            type = :includes
          end

          column = if type == :unique
            column.split(',').map{|c| c.to_sym}
          else
            column.to_sym
          end

          a = [:"validates_#{type}"]
          if arg
            a << arg
          end 
          a << column
          unless opts.empty?
            a << opts
          end
          a
        end

        # Return a range of integers assuming the argument is in
        # 1..2 or 1...2 format.
        def constraint_validation_int_range(arg)
          arg =~ /(\d+)\.\.(\.)?(\d+)/
          Range.new($1.to_i, $3.to_i, $2 == '.')
        end

        # Transform the LIKE pattern string argument into a
        # Regexp argument suitable for use with validates_format.
        def constraint_validation_like_to_regexp(arg, case_insensitive)
          arg = Regexp.escape(arg).gsub(/%%|%|_/) do |s|
            case s
            when '%%'
              '%'
            when '%'
              '.*'
            when '_'
              '.'
            end
          end
          arg = "\\A#{arg}\\z"

          if case_insensitive
            Regexp.new(arg, Regexp::IGNORECASE)
          else
            Regexp.new(arg)
          end
        end
      end

      module InstanceMethods
        # Run all of the constraint validations parsed from the database
        # when validating the instance.
        def validate
          super
          model.constraint_validations.each do |v|
            send(*v)
          end
        end
      end
    end
  end
end
