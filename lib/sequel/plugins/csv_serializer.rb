if RUBY_VERSION < "1.9"
  begin
    require 'fastercsv'
  rescue LoadError
    warn("plugin csv_serializer requires either ruby>=1.9 or fastercsv")
  end
else
  require 'csv'
end

module Sequel
  module Plugins
    # csv_serializer handles serializing entire Sequel::Model objects to CSV,
    # as well as support for deserializing CSV directly into Sequel::Model
    # objects.  It requires either the csv standard library from ruby >= 1.9,
    # or the fastercsv gem.
    #
    # Basic Example:
    #
    #   album = Album[1]
    #   album.to_csv(:write_headers=>true)
    #   # => "id,name,artist_id\n1,RF,2\n"
    #
    # You can provide options to control the CSV output:
    #
    #   album.to_csv(:only=>:name)
    #   album.to_csv(:except=>[:id, :artist_id])
    #   # => "RF\n"
    #
    # +to_csv+ also exists as a class and dataset method, both of which return
    # all objects in the dataset:
    #
    #   Album.to_csv
    #   Album.filter(:artist_id=>1).to_csv
    #
    # If you have an existing array of model instance you want to convert to
    # CSV, you can call the class to_csv method with the :array option:
    #
    #   Album.to_csv(:array=>[Album[1], Album[2]])
    #
    # In addition to creating CSV, this plugin also enables Sequel::Model
    # classes to create instances directly from CSV using the from_csv class
    # method:
    #
    #   csv = album.to_csv
    #   album = Album.from_csv(csv)
    #
    # The array_from_csv class method exists to parse arrays of model instances
    # from CSV:
    #
    #   csv = Album.filter(:artist_id=>1).to_csv
    #   albums = Album.array_from_csv(csv)
    #
    # These do not necessarily round trip, since doing so would let users
    # create model objects with arbitrary values.  By default, from_csv will
    # call set with the values in the hash.  If you want to specify the allowed
    # fields, you can use the :headers option.
    #
    #   Album.from_csv(album.to_csv, :headers=>%w'id name')
    #
    # If you want to update an existing instance, you can use the from_csv
    # instance method:
    #
    #   album.from_csv(csv)
    #
    # Usage:
    #
    #   # Add CSV output capability to all model subclass instances (called
    #   # before loading subclasses)
    #   Sequel::Model.plugin :csv_serializer
    #
    #   # Add CSV output capability to Album class instances
    #   Album.plugin :csv_serializer
    module CsvSerializer
      CSV = Object.const_defined?(:CSV) ? ::CSV : ::FasterCSV

      # Set up the column readers to do deserialization and the column writers
      # to save the value in deserialized_values
      def self.configure(model, opts = {})
        model.instance_eval do
          @csv_serializer_opts = (@csv_serializer_opts || {}).merge(opts)
        end
      end

      # Convert the options hash to one that can be passed to CSV.new
      def self.process_opts(model, opts = {}, columns = model.columns)
        opts = (model.csv_serializer_opts || {}).merge(opts)
        headers = if opts[:only].is_a?(Array)
                    opts.delete(:only)
                  elsif opts[:only].is_a?(Symbol)
                    [opts.delete(:only)]
                  elsif opts[:headers].is_a?(Array)
                    opts[:headers]
                  else
                    columns
                  end
        headers += Array(opts.delete(:include))
        headers -= Array(opts.delete(:except))
        opts = opts.merge(:headers=>headers)
        Array(opts.delete(:except)).each { |header| headers.delete(header) }
        opts
      end

      module ClassMethods
        # The default opts to use when serializing model objects to CSV
        attr_reader :csv_serializer_opts

        # Attempt to parse a single instance from the given CSV string
        def from_csv(csv, opts = {})
          values_hash = hash_from_csv(csv, opts)
          instance = new
          instance.set(values_hash)
          instance
        end

        # Attempt to parse a single line of CSV into a hash which can be
        # converted to a Sequel::Model instance
        def hash_from_csv(csv, opts = {})
          opts = CsvSerializer.process_opts(self, opts)

          types = {}
          db_schema.each_pair { |k, v| types[k] = v[:type] }

          values_hash = {}
          CSV.parse_line(csv, opts).to_hash.each_pair do |k, v|
            next if v.nil? || k.nil?

            value = if types[k]
                      db.send(:column_schema_default_to_ruby_value, v, types[k])
                    else
                      v
                    end

            values_hash[k] = value
          end
          values_hash
        end

        # Attempt to parse an array of instances from the given CSV string
        def array_from_csv(csv, opts = {})
          opts = CsvSerializer.process_opts(self, opts)

          instances = []
          CSV.parse(csv, opts) do |row|
            instance = new
            row.to_hash.each_pair do |k, v|
              instance.send(:"#{k}=", v)
            end
            instances << instance
          end
          instances
        end

        Plugins.inherited_instance_variables(
          self, :@csv_serializer_opts => lambda do |csv_serializer_opts|
            opts = {}
            csv_serializer_opts.each do |k, v|
              opts[k] = (v.is_a?(Array) || v.is_a?(Hash)) ? v.dup : v
            end
            opts
          end)

        Plugins.def_dataset_methods(self, :to_csv)
      end # module ClassMethods

      module InstanceMethods
        # Parse the provided CSV
        def from_csv(csv, opts = {})
          values_hash = self.class.hash_from_csv(csv, opts)
          set(values_hash)
        end

        # Return a string in CSV format.  Accepts the same options as CSV.new,
        # as well as the following options:
        #
        # :except :: Symbol or Array of Symbols of columns not to include in
        #            the CSV output.
        # :only :: Symbol or Array of Symbols of columns to include in the CSV
        #          output, ignoring all other columns
        # :include :: Symbol or Array of Symbols specifying non-column
        #             attributes to include in the CSV output.
        def to_csv(opts = {})
          opts = CsvSerializer.process_opts(model, opts)

          CSV.generate(opts) do |csv|
            csv << opts[:headers].map { |k| send(k) }
          end
        end
      end # module InstanceMethods

      module DatasetMethods
        # Return a CSV string representing an array of all objects in this
        # dataset.  Takes the same options as the instance method, and passes
        # them to every instance.  Accepts the same options as CSV.new, as well
        # as the following options:
        #
        # :array :: An array of instances.  If this is not provided, calls #all
        #           on the receiver to get the array.
        def to_csv(opts = {})
          opts = CsvSerializer.process_opts(model, opts, columns)
          items = opts.delete(:array) || self

          CSV.generate(opts) do |csv|
            items.each do |object|
              csv << opts[:headers].map { |header| object[header] }
            end
          end
        end
      end # module DatasetMethods
    end # module CsvSerializer
  end # module Plugins
end # module Sequel
