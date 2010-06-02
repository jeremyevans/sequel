module Sequel
  tsk_require 'json'

  module Plugins
    # The json_serializer plugin handles deserializing entire Sequel::Model
    # objects to JSON.  It requires the json library, can can work with either
    # the pure ruby version or the C extension.
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
    # In addition to creating JSON, this plugin also enables Sequel::Model
    # objects to be automatically created when JSON is parsed:
    #
    #   json = album.to_json
    #   album = JSON.parse(json)
    #
    # In addition, you can update existing model objects directly from JSON
    # using +from_json+:
    #
    #   album.from_json(json)
    #
    # This works by parsing the JSON (which should return a hash), and then
    # calling +set+ with the returned hash.
    #
    # Additionally, +to_json+ also exists as a class and dataset method, both
    # of which return all objects in the dataset:
    #
    #   Album.to_json
    #   Album.filter(:artist_id=>1).to_json(:include=>:tags)
    #
    # Usage:
    #
    #   # Add JSON output capability to all model subclass instances (called before loading subclasses)
    #   Sequel::Model.plugin :json_serializer
    #
    #   # Add JSON output capability to Album class instances
    #   Album.plugin :json_serializer
    module JsonSerializer
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
        # Create a new model object from the hash provided by parsing
        # JSON.  Handles column values (stored in +values+), associations
        # (stored in +associations+), and other values (by calling a
        # setter method).  If an entry in the hash is not a column or
        # an association, and no setter method exists, raises an Error.
        def json_create(hash)
          obj = new
          cols = columns.map{|x| x.to_s}
          assocs = associations.map{|x| x.to_s}
          meths = obj.send(:setter_methods, nil, nil)
          hash.delete(JSON.create_id)
          hash.each do |k, v|
            if assocs.include?(k)
              obj.associations[k.to_sym] = v
            elsif cols.include?(k)
              obj.values[k.to_sym] = v
            elsif meths.include?("#{k}=")
              obj.send("#{k}=", v)
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
      end

      module InstanceMethods
        # Parse the provided JSON, which should return a hash,
        # and call +set+ with that hash.
        def from_json(json)
          set(JSON.parse(json))
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
        # :naked :: Not to add the JSON.create_id key to the JSON
        #           output hash, so when the JSON is parsed, it
        #           will yield a hash instead of a model object.
        # :only :: Symbol or Array of Symbols of columns to only
        #          include in the JSON output, ignoring all other
        #          columns.
        def to_json(*a)
          if opts = a.first.is_a?(Hash)
            opts = a.first
            a = []
          else
            opts = {}
          end
          vals = values
          cols = if only = opts[:only]
            Array(only)
          else
            vals.keys - Array(opts[:except])
          end
          h = (JSON.create_id && !opts[:naked]) ? {JSON.create_id=>model.name} : {}
          cols.each{|c| h[c.to_s] = vals[c]}
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
          h.to_json(*a)
        end
      end

      module DatasetMethods
        # Return a JSON string representing an array of all objects in
        # this dataset.  Takes the same options as the the instance
        # method, and passes them to every instance.
        def to_json(*a)
          if a.first.is_a?(Hash)
            opts = a.first
            a = []
          else
            opts = {}
          end
          if row_proc
            all.map{|obj| Literal.new(obj.to_json(opts))}.to_json(*a)
          else
            all.to_json(*a)
          end
        end
      end
    end
  end
end
