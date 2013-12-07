---
 layout: post
 title: JSON and XML Serializers
---

Sequel has finally joined the web services party!  Some recent commits have added both a <a href="http://github.com/jeremyevans/sequel/blob/master/lib/sequel/plugins/json_serializer.rb">JSON serializer</a> and an <a href="http://github.com/jeremyevans/sequel/blob/master/lib/sequel/plugins/xml_serializer.rb">XML serializer</a>.  Sequel relies on the <a href="http://flori.github.com/json/">json</a> and <a href="http://nokogiri.org/">nokogiri</a> libraries for JSON and XML support, respectively.

As you might expect, these plugins allow you to take a model object and serialize it to either JSON or XML:

    puts Artist.first.to_json
    # Output:
    {"name":"YJM","json_class":"Artist","id":2}

    puts Artist.first.to_xml
    # Output:
    <?xml version="1.0"?>
    <artist>
      <name>YJM</name>
      <id>2</id>
    </artist>

Similar to ActiveRecord, both to_json and to_xml take :only and :except options for specifying which columns should be included or excluded:


    puts Artist.first.to_json(:only=>:name)
    # Output:
    {"name":"YJM","json_class":"Artist"}

    puts Artist.first.to_xml(:except=>:id)
    # Output:
    <?xml version="1.0"?>
    <artist>
      <name>YJM</name>
    </artist>

You can also use :include to specify other methods or associations to include in the output:

    puts Album.first.to_json(:include=>:artist)
    # Output:
    {"name":"RF","json_class":"Album","id":1,"artist_id":2,
     "artist":{"name":"YJM","json_class":"Artist","id":2\}\}

    puts Album.first.to_xml(:include=>[:artist, :query])
    # Output:
    <?xml version="1.0"?>
    <album>
      <name>RF</name>
      <id>1<1id>
      <artist_id>2</artist_id>
      <artist>
        <name>YJM</name>
        <id>2</id>
      </artist>
      <query>http://www.google.com/search?q=rf</query>
    </album>

You can use a hash as an :include value to pass options to associations:

    puts Album.first.to_json(:include=>{:artist=>{:only=>:name\}\})
    # Output:
    {"name":"RF","json_class":"Album","id":1,"artist_id":2,
     "artist":{"name":"YJM","json_class":"Artist"}

    puts Artist.first.to_xml(:include=>{:albums=>{:include=>:tags\}\})
    # Output:
    <?xml version="1.0"?>
    <artist>
      <name>YJM</name>
      <id>2</id>
      <albums>
        <album>
          <name>RF</name>
          <id>1<1id>
          <artist_id>2</artist_id>
          <tags>
            <tag>
              <name>Metal</name>
              <id>3</id>
            </tag>
            <tag>
              <name>Rock</name>
              <id>4</id>
            </tag>
          </tags>
        </album>
      </albums>
    </artist>

Both of the plugins allow you to export entire datasets as JSON or XML, which will retrieve all of the objects in the dataset and then format them:

    puts Artist.to_json
    # Output:
    [{"name":"YJM","json_class":"Artist","id":2}]

    puts Artist.to_xml
    # Output:
    <?xml version="1.0"?>
    <artists>
      <artist>
        <name>YJM</name>
        <id>2</id>
      </artist>
    </artists>

While the plugin names imply only serialization, both plugins handle deserializing their respective formats back to model objects.  For JSON, you just use the standard JSON.parse, which will automatically create model objects from JSON:

    JSON.parse(Artist.first.to_json) # == Artist.first
    JSON.parse(Artist.to_json) # == Artist.all

XML can't be loaded the same way, as parsing it into ruby data structures requires context that it doesn't have.  Sequel provides the from_xml and array_from_xml class methods for loading single objects or arrays of objects:

    Artist.from_xml(Artist.first.to_xml) # == Artist.first
    Artist.array_from_xml(Artist.to_xml) # == Artist.all

Sequel also offers from_json and from_xml instance methods for updating specific objects:

    artist = Artist.first
    artist.from_json(json)
    artist.from_xml(xml)

The API and many of the options are based on ActiveRecord, who led the way in this area.  The implementation is probably very different, though. 

There are some other methods and options not covered in this post, so if you want more details, look at the RDoc comments for the plugins and their methods.

