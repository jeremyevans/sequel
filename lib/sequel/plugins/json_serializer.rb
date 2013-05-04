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
    # The array_from_json class method exists to parse arrays of model instances
    # from json:
    #
    #   json = Album.filter(:artist_id=>1).to_json
    #   albums = Album.array_from_json(json)
    #
    # These does not necessarily round trip, since doing so would let users
    # create model objects with arbitrary values.  By default, from_json will
    # call set with the values in the hash.  If you want to specify the allowed
    # fields, you can use the :fields option, which will call set_fields with
    # the given fields:
    #
    #   Album.from_json(album.to_json, :fields=>%w'id name')
    #
    # If you want to update an existing instance, you can use the from_json
    # instance method:
    #
    #   album.from_json(json)
    #
    # Both of these allow creation of cached associated objects, if you provide
    # the :associations option:
    #
    #   album.from_json(json, :associations=>:artist)
    #
    # You can even provide options when setting up the associated objects:
    #
    #   album.from_json(json, :associations=>{:artist=>{:fields=>%w'id name', :associations=>:tags}})
    #
    # If the json is trusted and should be allowed to set all column and association
    # values, you can use the :all_columns and :all_associations options.
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

        # Attempt to parse a single instance from the given JSON string,
        # with options passed to InstanceMethods#from_json_node.
        def from_json(json, opts={})
          if opts[:all_associations] || opts[:all_columns]
            Sequel::Deprecation.deprecate("The from_json :all_associations and :all_columns", 'You need to explicitly specify the associations and columns via the :associations and :fields options')
          end
          v = Sequel.parse_json(json)
          case v
          when self
            v
          when Hash
            new.from_json_node(v, opts)
          else
            raise Error, "parsed json doesn't return a hash or instance of #{self}"
          end
        end

        # Attempt to parse an array of instances from the given JSON string,
        # with options passed to InstanceMethods#from_json_node.
        def array_from_json(json, opts={})
          if opts[:all_associations] || opts[:all_columns]
            Sequel::Deprecation.deprecate("The from_json :all_associations and :all_columns", 'You need to explicitly specify the associations and columns via the :associations and :fields options')
          end
          v = Sequel.parse_json(json)
          if v.is_a?(Array)
            raise(Error, 'parsed json returned an array containing non-hashes') unless v.all?{|ve| ve.is_a?(Hash) || ve.is_a?(self)}
            v.map{|ve| ve.is_a?(self) ? ve : new.from_json_node(ve, opts)}
          else
            raise(Error, 'parsed json did not return an array')
          end
        end

        # Exists for compatibility with old json library which allows creation
        # of arbitrary ruby objects by JSON.parse.  Creates a new instance
        # and populates it using InstanceMethods#from_json_node with the
        # :all_columns and :all_associations options.  Not recommended for usage
        # in new code, consider calling the from_json method directly with the JSON string.
        def json_create(hash, opts={})
          Sequel::Deprecation.deprecate("Model.json_create", 'Switch to Model.from_json')
          new.from_json_node(hash, {:all_columns=>true, :all_associations=>true}.merge(opts))
        end

        Plugins.inherited_instance_variables(self, :@json_serializer_opts=>lambda do |json_serializer_opts|
          opts = {}
          json_serializer_opts.each{|k, v| opts[k] = (v.is_a?(Array) || v.is_a?(Hash)) ? v.dup : v}
          opts
        end)

        Plugins.def_dataset_methods(self, :to_json)
      end

      module InstanceMethods
        # Parse the provided JSON, which should return a hash,
        # and process the hash with from_json_node.
        def from_json(json, opts={})
          if opts[:all_associations] || opts[:all_columns]
            Sequel::Deprecation.deprecate("The from_json :all_associations and :all_columns", 'You need to explicitly specify the associations and columns via the :associations and :fields options')
          end
          from_json_node(Sequel.parse_json(json), opts)
        end

        # Using the provided hash, update the instance with data contained in the hash. By default, just
        # calls set with the hash values.
        # 
        # Options:
        # :all_associations :: Indicates that all associations supported by the model should be tried.
        #                      This option also cascades to associations if used. It is better to use the
        #                      :associations option instead of this option. This option only exists for
        #                      backwards compatibility.
        # :all_columns :: Overrides the setting logic allowing all setter methods be used,
        #                 even if access to the setter method is restricted.
        #                 This option cascades to associations if used, and can be reset in those associations
        #                 using the :all_columns=>false or :fields options.  This option is considered a
        #                 security risk, and only exists for backwards compatibility.  It is better to use
        #                 the :fields option appropriately instead of this option, or no option at all.
        # :associations :: Indicates that the associations cache should be updated by creating
        #                  a new associated object using data from the hash.  Should be a Symbol
        #                  for a single association, an array of symbols for multiple associations,
        #                  or a hash with symbol keys and dependent association option hash values.
        # :fields :: Changes the behavior to call set_fields using the provided fields, instead of calling set.
        def from_json_node(hash, opts={})
          unless hash.is_a?(Hash)
            raise Error, "parsed json doesn't return a hash"
          end
          hash.delete(JSON.create_id)

          unless assocs = opts[:associations]
            if opts[:all_associations]
              assocs = {}
              model.associations.each{|v| assocs[v] = {:all_associations=>true}}
            end
          end

          if assocs
            assocs = case assocs
            when Symbol
              {assocs=>{}}
            when Array
              assocs_tmp = {}
              assocs.each{|v| assocs_tmp[v] = {}}
              assocs_tmp
            when Hash
              assocs
            else
              raise Error, ":associations should be Symbol, Array, or Hash if present"
            end

            if opts[:all_columns]
              assocs.each_value do |assoc_opts|
                assoc_opts[:all_columns] = true unless assoc_opts.has_key?(:fields) || assoc_opts.has_key?(:all_columns)
              end
            end

            assocs.each do |assoc, assoc_opts|
              if assoc_values = hash.delete(assoc.to_s)
                unless r = model.association_reflection(assoc)
                  raise Error, "Association #{assoc} is not defined for #{model}"
                end

                associations[assoc] = if r.returns_array?
                  raise Error, "Attempt to populate array association with a non-array" unless assoc_values.is_a?(Array)
                  assoc_values.map{|v| v.is_a?(r.associated_class) ? v : r.associated_class.new.from_json_node(v, assoc_opts)}
                else
                  raise Error, "Attempt to populate non-array association with an array" if assoc_values.is_a?(Array)
                  assoc_values.is_a?(r.associated_class) ? assoc_values : r.associated_class.new.from_json_node(assoc_values, assoc_opts)
                end
              end
            end
          end

          if fields = opts[:fields]
            set_fields(hash, fields, opts)
          elsif opts[:all_columns]
            meths = methods.collect{|x| x.to_s}.grep(Model::SETTER_METHOD_REGEXP) - Model::RESTRICTED_SETTER_METHODS
            hash.each do |k, v|
              if meths.include?(setter_meth = "#{k}=")
                send(setter_meth, v)
              else
                raise Error, "Entry in JSON does not have a matching setter method: #{k}"
              end
            end
          else
            set(hash)
          end

          self
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
          when :both
            true
          else
            Sequel::Deprecation.deprecate('The to_json :root=>true option will mean :root=>:collection starting in Sequel 4.  Use :root=>:both if you want to wrap both the collection and each item in a wrapper.')
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
