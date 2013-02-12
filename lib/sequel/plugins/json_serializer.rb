module Sequel
  tsk_require 'json'

  module Plugins
    # The json_serializer plugin handles serializing entire Sequel::Model
    # objects to JSON, as well as support for deserializing JSON directly
    # into Sequel::Model objects.  It requires the json library, and can
    # work with either the pure ruby version or the C extension.
    #
    # Basic Example:
    #
    #   album = Album[1]
    #   album.to_json
    #   # => '{"json_class"=>"Album","id"=>1,"name"=>"RF","artist_id"=>2}'
    #
    # In addition, you can provide options to control the JSON output:
    #
    #   album.to_json(:only=>:name)
    #   album.to_json(:except=>[:id, :artist_id])
    #   # => '{"json_class"="Album","name"=>"RF"}'
    #
    #   album.to_json(:include=>:artist)
    #   # => '{"json_class":"Album","id":1,"name":"RF","artist_id":2,
    #          "artist":{"json_class":"Artist","id":2,"name":"YJM"}}'
    # 
    # You can use a hash value with <tt>:include</tt> to pass options
    # to associations:
    #
    #   album.to_json(:include=>{:artist=>{:only=>:name}})
    #   # => '{"json_class":"Album","id":1,"name":"RF","artist_id":2,
    #          "artist":{"json_class":"Artist","name":"YJM"}}'
    #
    # You can specify the <tt>:root</tt> option to nest the JSON under the
    # name of the model:
    #
    #   album.to_json(:root => true)
    #   # => '{"album":{"id":1,"name":"RF","artist_id":2}}'
    #
    # Additionally, +to_json+ also exists as a class and dataset method, both
    # of which return all objects in the dataset:
    #
    #   Album.to_json
    #   Album.filter(:artist_id=>1).to_json(:include=>:tags)
    #
    # If you have an existing array of model instances you want to convert to
    # JSON, you can call the class to_json method with the :array option:
    #
    #   Album.to_json(:array=>[Album[1], Album[2]])
    #
    # In addition to creating JSON, this plugin also enables Sequel::Model
    # classes to create instances directly from JSON using the from_json class
    # method:
    #
    #   json = album.to_json
    #   album = Album.from_json(json)
    #
    # This should be able to roundtrip, such that:
    #
    #   Album.from_json(album.to_json) == album
    #   Album.from_json(Album.order(:id).to_json) == Album.order(:id).all
    #
    # However, you should be extremely careful when using untrusted JSON
    # input. The from_json class method can set any column values in the object,
    # and can set arbitrary cached associations. You should only use the from_json
    # class method if you are externally validating the input.
    #
    # A safer method is the #from_json instance method:
    #
    #   album.from_json(json)
    #
    # This works by parsing the JSON (which should return a hash), and then
    # calling +set+ or +set_fields+ with the returned hash, and doesn't allow
    # arbitrary column values or cached associations to be set.
    #
    # Note that active_support/json makes incompatible changes to the to_json API,
    # and breaks some aspects of the json_serializer plugin.  You can undo the damage
    # done by active_support/json by doing:
    #
    #   class Array
    #     def to_json(options = {})
    #       JSON.generate(self)
    #     end
    #   end
    #
    #   class Hash
    #     def to_json(options = {})
    #       JSON.generate(self)
    #     end
    #   end
    #
    # Note that this will probably cause active_support/json to no longer work
    # correctly in some cases.
    #
    # Usage:
    #
    #   # Add JSON output capability to all model subclass instances (called before loading subclasses)
    #   Sequel::Model.plugin :json_serializer
    #
    #   # Add JSON output capability to Album class instances
    #   Album.plugin :json_serializer
    module JsonSerializer
      # Set up the column readers to do deserialization and the column writers
      # to save the value in deserialized_values.
      def self.configure(model, opts={})
        model.instance_eval do
          @json_serializer_opts = (@json_serializer_opts || {}).merge(opts)
        end
      end
      
      # Helper class used for making sure that cascading options
      # for model associations works correctly.  Cascaded options
      # work by creating instances of this class, which take a
      # literal JSON string and have +to_json+ return it.
      class Literal
        # Store the literal JSON to use
        def initialize(json)
          @json = json
        end
        
        # Return the literal JSON to use
        def to_json(*a)
          @json
        end
      end

      module ClassMethods
        # The default opts to use when serializing model objects to JSON.
        attr_reader :json_serializer_opts

        # Attempt to parse a single instance from the given JSON string.
        def from_json(json)
          v = Sequel.parse_json(json)
          case v
          when self
            v
          when Hash
            json_create(v)
          else
            raise Error, "parsed json doesn't return a hash or instance of #{self}"
          end
        end

        # Attempt to parse an array of instances from the given JSON string.
        def array_from_json(json)
          v = Sequel.parse_json(json)
          if v.is_a?(Array)
            raise(Error, 'parsed json returned an array containing non-hashes') unless v.all?{|ve| ve.is_a?(Hash) || ve.is_a?(self)}
            v.map{|ve| ve.is_a?(self) ? ve : json_create(ve)}
          else
            raise(Error, 'parsed json did not return an array')
          end
        end

        # Create a new model object from the hash provided by parsing
        # JSON.  Handles column values (stored in +values+), associations
        # (stored in +associations+), and other values (by calling a
        # setter method).  If an entry in the hash is not a column or
        # an association, and no setter method exists, raises an Error.
        def json_create(hash)
          unless hash.is_a?(Hash)
            raise Error, "json_create argument must be a hash"
          end

          obj = new
          cols = columns.map{|x| x.to_s}
          assocs = {}
          association_reflections.each{|name, r| assocs[name.to_s] = r}
          meths = obj.send(:setter_methods, nil, nil)
          hash.delete(JSON.create_id)
          hash.each do |k, v|
            if r = assocs[k]
              obj.associations[k.to_sym] = if v.is_a?(Array)
                raise Error, "Attempt to populate non-array association with an array" unless r.returns_array?
                v.map{|ve| r.associated_class.json_create(ve)}
              else
                raise Error, "Attempt to populate array association with a non-array" if r.returns_array?
                r.associated_class.json_create(v)
              end
            elsif meths.include?("#{k}=")
              obj.send("#{k}=", v)
            elsif cols.include?(k)
              obj.values[k.to_sym] = v
            else
              raise Error, "Entry in JSON hash not an association or column and no setter method exists: #{k}"
            end
          end
          obj
        end

        # Call the dataset +to_json+ method.
        def to_json(*a)
          dataset.to_json(*a)
        end
        
        # Copy the current model object's default json options into the subclass.
        def inherited(subclass)
          super
          opts = {}
          json_serializer_opts.each{|k, v| opts[k] = (v.is_a?(Array) || v.is_a?(Hash)) ? v.dup : v}
          subclass.instance_variable_set(:@json_serializer_opts, opts)
        end
      end

      module InstanceMethods
        # Parse the provided JSON, which should return a hash,
        # and call +set+ with that hash.
        def from_json(json, opts={})
          h = Sequel.parse_json(json)
          unless h.is_a?(Hash)
            raise Error, "parsed json doesn't return a hash"
          end

          if fields = opts[:fields]
            set_fields(h, fields, opts)
          else
            set(h)
          end
        end

        # Return a string in JSON format.  Accepts the following
        # options:
        #
        # :except :: Symbol or Array of Symbols of columns not
        #            to include in the JSON output.
        # :include :: Symbol, Array of Symbols, or a Hash with
        #             Symbol keys and Hash values specifying
        #             associations or other non-column attributes
        #             to include in the JSON output.  Using a nested
        #             hash, you can pass options to associations
        #             to affect the JSON used for associated objects.
        # :naked :: Not to add the JSON.create_id (json_class) key to the JSON
        #           output hash, so when the JSON is parsed, it
        #           will yield a hash instead of a model object.
        # :only :: Symbol or Array of Symbols of columns to only
        #          include in the JSON output, ignoring all other
        #          columns.
        # :root :: Qualify the JSON with the name of the object.
        #          Implies :naked since the object name is explicit.
        def to_json(*a)
          if opts = a.first.is_a?(Hash)
            opts = model.json_serializer_opts.merge(a.first)
            a = []
          else
            opts = model.json_serializer_opts
          end
          vals = values
          cols = if only = opts[:only]
            Array(only)
          else
            vals.keys - Array(opts[:except])
          end
          h = (JSON.create_id && !opts[:naked] && !opts[:root]) ? {JSON.create_id=>model.name} : {}
          cols.each{|c| h[c.to_s] = send(c)}
          if inc = opts[:include]
            if inc.is_a?(Hash)
              inc.each do |k, v|
                v = v.empty? ? [] : [v]
                h[k.to_s] = case objs = send(k)
                when Array
                  objs.map{|obj| Literal.new(obj.to_json(*v))}
                else
                  Literal.new(objs.to_json(*v))
                end
              end
            else
              Array(inc).each{|c| h[c.to_s] = send(c)}
            end
          end
          h = {model.send(:underscore, model.to_s) => h} if opts[:root]
          h.to_json(*a)
        end
      end

      module DatasetMethods
        # Return a JSON string representing an array of all objects in
        # this dataset.  Takes the same options as the the instance
        # method, and passes them to every instance.  Additionally,
        # respects the following options:
        #
        # :array :: An array of instances.  If this is not provided,
        #           calls #all on the receiver to get the array.
        # :root :: If set to :collection, only wraps the collection
        #          in a root object.  If set to :instance, only wraps
        #          the instances in a root object.  If set to :both,
        #          wraps both the collection and instances in a root
        #          object.  Unfortunately, for backwards compatibility,
        #          if this option is true and doesn't match one of those
        #          symbols, it defaults to both.  That may change in a
        #          future version, so for forwards compatibility, you
        #          should pick a specific symbol for your desired
        #          behavior.
        def to_json(*a)
          if opts = a.first.is_a?(Hash)
            opts = model.json_serializer_opts.merge(a.first)
            a = []
          else
            opts = model.json_serializer_opts
          end

          collection_root = case opts[:root]
          when nil, false, :instance
            false
          when :collection
            opts = opts.dup
            opts.delete(:root)
            opts[:naked] = true unless opts.has_key?(:naked)
            true
          else
            true
          end

          res = if row_proc 
            array = if opts[:array]
              opts = opts.dup
              opts.delete(:array)
            else
              all
            end
            array.map{|obj| Literal.new(obj.to_json(opts))}
           else
            all
          end

          if collection_root
            {model.send(:pluralize, model.send(:underscore, model.to_s)) => res}.to_json(*a)
          else
            res.to_json(*a)
          end
        end
      end
    end
  end
end
