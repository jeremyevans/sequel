module Sequel
  tsk_require 'nokogiri'

  module Plugins
    # The xml_serializer plugin handles serializing entire Sequel::Model
    # objects to XML, and deserializing XML into a single Sequel::Model
    # object or an array of Sequel::Model objects.  It requires the
    # nokogiri library.
    #
    # Basic Example:
    #
    #   album = Album[1]
    #   puts album.to_xml
    #   # Output:
    #   <?xml version="1.0"?>
    #   <album>
    #     <id>1</id>
    #     <name>RF</name>
    #     <artist_id>2</artist_id>
    #   </album>
    #
    # You can provide options to control the XML output:
    #
    #   puts album.to_xml(:only=>:name)
    #   puts album.to_xml(:except=>[:id, :artist_id])
    #   # Output:
    #   <?xml version="1.0"?>
    #   <album>
    #     <name>RF</name>
    #   </album>
    #
    #   album.to_xml(:include=>:artist)
    #   # Output:
    #   <?xml version="1.0"?>
    #   <album>
    #     <id>1</id>
    #     <name>RF</name>
    #     <artist_id>2</artist_id>
    #     <artist>
    #       <id>2</id>
    #       <name>YJM</name>
    #     </artist>
    #   </album>
    # 
    # You can use a hash value with <tt>:include</tt> to pass options
    # to associations:
    #
    #   album.to_json(:include=>{:artist=>{:only=>:name}})
    #   # Output:
    #   <?xml version="1.0"?>
    #   <album>
    #     <id>1</id>
    #     <name>RF</name>
    #     <artist_id>2</artist_id>
    #     <artist>
    #       <name>YJM</name>
    #     </artist>
    #   </album>
    #
    # In addition to creating XML, this plugin also enables Sequel::Model
    # objects to be created by parsing XML:
    #
    #   xml = album.to_xml
    #   album = Album.from_xml(xml)
    #
    # In addition, you can update existing model objects directly from XML
    # using +from_xml+:
    #
    #   album.from_xml(xml)
    #
    # Additionally, +to_xml+ also exists as a class and dataset method, both
    # of which return all objects in the dataset:
    #
    #   Album.to_xml
    #   Album.filter(:artist_id=>1).to_xml(:include=>:tags)
    #
    # Such XML can be loaded back into an array of Sequel::Model objects using
    # +array_from_xml+:
    #
    #   Album.array_from_xml(Album.to_xml) # same as Album.all
    #
    # Usage:
    #
    #   # Add XML output capability to all model subclass instances (called before loading subclasses)
    #   Sequel::Model.plugin :xml_serializer
    #
    #   # Add XML output capability to Album class instances
    #   Album.plugin :xml_serializer
    module XmlSerializer
      module ClassMethods
        # Proc that camelizes the input string, used for the :camelize option
        CAMELIZE = proc{|s| s.camelize}

        # Proc that dasherizes the input string, used for the :dasherize option
        DASHERIZE = proc{|s| s.dasherize}

        # Proc that returns the input string as is, used if
        # no :name_proc, :dasherize, or :camelize option is used.
        IDENTITY = proc{|s| s}

        # Proc that underscores the input string, used for the :underscore option
        UNDERSCORE = proc{|s| s.underscore}

        # Return an array of instances of this class based on
        # the provided XML.
        def array_from_xml(xml, opts={})
          Nokogiri::XML(xml).children.first.children.reject{|c| c.is_a?(Nokogiri::XML::Text)}.map{|c| from_xml_node(c, opts)}
        end

        # Return an instance of this class based on the provided
        # XML.
        def from_xml(xml, opts={})
          from_xml_node(Nokogiri::XML(xml).children.first, opts)
        end

        # Return an instance of this class based on the given
        # XML node, which should be Nokogiri::XML::Node instance.
        # This should probably not be used directly by user code.
        def from_xml_node(parent, opts={})
          new.from_xml_node(parent, opts)
        end

        # Call the dataset +to_xml+ method.
        def to_xml(opts={})
          dataset.to_xml(opts)
        end

        # Return an appropriate Nokogiri::XML::Builder instance
        # used to create the XML.  This should probably not be used
        # directly by user code.
        def xml_builder(opts={})
          if opts[:builder]
            opts[:builder]
          else
            builder_opts = if opts[:builder_opts]
              opts[:builder_opts]
            else
              {}
            end
            builder_opts[:encoding] = opts[:encoding] if opts.has_key?(:encoding)
            Nokogiri::XML::Builder.new(builder_opts)
          end
        end

        # Return a proc (or any other object that responds to []),
        # used for formatting XML tag names when serializing to XML.
        # This should probably not be used directly by user code.
        def xml_deserialize_name_proc(opts={})
          if opts[:name_proc]
            opts[:name_proc]
          elsif opts[:underscore]
            UNDERSCORE
          else
            IDENTITY
          end
        end

        # Return a proc (or any other object that responds to []),
        # used for formatting XML tag names when serializing to XML.
        # This should probably not be used directly by user code.
        def xml_serialize_name_proc(opts={})
          pr = if opts[:name_proc]
            opts[:name_proc]
          elsif opts[:dasherize]
            DASHERIZE
          elsif opts[:camelize]
            CAMELIZE
          else
            IDENTITY
          end
          proc{|s| "#{pr[s]}_"}
        end
      end

      module InstanceMethods
        # Update the contents of this instance based on the given XML.
        # Accepts the following options:
        #
        # :name_proc :: Proc or Hash that accepts a string and returns
        #               a string, used to convert tag names to column or
        #               association names.
        # :underscore :: Sets the :name_proc option to one that calls +underscore+
        #                on the input string.  Requires that you load the inflector
        #                extension or another library that adds String#underscore.
        def from_xml(xml, opts={})
          from_xml_node(Nokogiri::XML(xml).children.first, opts)
        end

        # Update the contents of this instance based on the given 
        # XML node, which should be a Nokogiri::XML::Node instance.
        def from_xml_node(parent, opts={})
          cols = model.columns.map{|x| x.to_s}
          assocs = {}
          model.associations.map{|x| assocs[x.to_s] = model.association_reflection(x)}
          meths = send(:setter_methods, nil, nil)
          name_proc = model.xml_deserialize_name_proc(opts)
          parent.children.each do |node|
            next if node.is_a?(Nokogiri::XML::Text)
            k = name_proc[node.name]
            if ar = assocs[k]
              klass = ar.associated_class
              associations[k.to_sym] = if ar.returns_array?
                node.children.reject{|c| c.is_a?(Nokogiri::XML::Text)}.map{|c| klass.from_xml_node(c)}
              else
                klass.from_xml_node(node)
              end
            elsif cols.include?(k)
              self[k.to_sym] = node[:nil] ? nil : node.children.first.to_s
            elsif meths.include?("#{k}=")
              send("#{k}=", node[:nil] ? nil : node.children.first.to_s)
            else
              raise Error, "Entry in XML not an association or column and no setter method exists: #{k}"
            end
          end
          self
        end

        # Return a string in XML format.  If a block is given, yields the XML
        # builder object so you can add additional XML tags.
        # Accepts the following options:
        #
        # :builder :: The builder instance used to build the XML,
        #             which should be an instance of Nokogiri::XML::Node.  This
        #             is necessary if you are serializing entire object graphs,
        #             like associated objects.
        # :builder_opts :: Options to pass to the Nokogiri::XML::Builder
        #                  initializer, if the :builder option is not provided.
        # :camelize:: Sets the :name_proc option to one that calls +camelize+
        #             on the input string.  Requires that you load the inflector
        #             extension or another library that adds String#camelize.
        # :dasherize :: Sets the :name_proc option to one that calls +dasherize+
        #               on the input string.  Requires that you load the inflector
        #               extension or another library that adds String#dasherize.
        # :encoding :: The encoding to use for the XML output, passed
        #              to the Nokogiri::XML::Builder initializer.
        # :except :: Symbol or Array of Symbols of columns not
        #            to include in the XML output.
        # :include :: Symbol, Array of Symbols, or a Hash with
        #             Symbol keys and Hash values specifying
        #             associations or other non-column attributes
        #             to include in the XML output.  Using a nested
        #             hash, you can pass options to associations
        #             to affect the XML used for associated objects.
        # :name_proc :: Proc or Hash that accepts a string and returns
        #               a string, used to format tag names.
        # :only :: Symbol or Array of Symbols of columns to only
        #          include in the JSON output, ignoring all other
        #          columns.
        # :root_name :: The base name to use for the XML tag that
        #               contains the data for this instance.  This will
        #               be the name of the root node if you are only serializing
        #               a single object, but not if you are serializing
        #               an array of objects using Model.to_xml or Dataset#to_xml.
        # :types :: Set to true to include type information for
        #           all of the columns, pulled from the db_schema.
        def to_xml(opts={})
          vals = values
          types = opts[:types]
          inc = opts[:include]

          cols = if only = opts[:only]
            Array(only)
          else
            vals.keys - Array(opts[:except])
          end

          name_proc = model.xml_serialize_name_proc(opts)
          x = model.xml_builder(opts)
          x.send(name_proc[opts.fetch(:root_name, model.send(:underscore, model.name)).to_s]) do |x1|
            cols.each do |c|
              attrs = {}
              if types
                attrs[:type] = db_schema.fetch(c, {})[:type]
              end
              v = vals[c]
              if v.nil?
                attrs[:nil] = ''
              end
              x1.send(name_proc[c.to_s], v, attrs)
            end
            if inc.is_a?(Hash)
              inc.each{|k, v| to_xml_include(x1, k, v)}
            else
              Array(inc).each{|i| to_xml_include(x1, i)}
            end
            yield x1 if block_given?
          end
          x.to_xml
        end

        private

        # Handle associated objects and virtual attributes when creating
        # the xml.
        def to_xml_include(node, i, opts={})
          name_proc = model.xml_serialize_name_proc(opts)
          objs = send(i)
          if objs.is_a?(Array) && objs.all?{|x| x.is_a?(Sequel::Model)}
            node.send(name_proc[i.to_s]) do |x2|
              objs.each{|obj| obj.to_xml(opts.merge(:builder=>x2))}
            end
          elsif objs.is_a?(Sequel::Model)
            objs.to_xml(opts.merge(:builder=>node, :root_name=>i))
          else
            node.send(name_proc[i.to_s], objs)
          end
        end
      end

      module DatasetMethods
        # Return an XML string containing all model objects specified with
        # this dataset.  Takes all of the options available to Model#to_xml,
        # as well as the :array_root_name option for specifying the name of
        # the root node that contains the nodes for all of the instances.
        def to_xml(opts={})
          raise(Sequel::Error, "Dataset#to_xml") unless row_proc
          x = model.xml_builder(opts)
          name_proc = model.xml_serialize_name_proc(opts)
          x.send(name_proc[opts.fetch(:array_root_name, model.send(:pluralize, model.send(:underscore, model.name))).to_s]) do |x1|
            all.each do |obj|
              obj.to_xml(opts.merge(:builder=>x1))
            end
          end
          x.to_xml
        end
      end
    end
  end
end
