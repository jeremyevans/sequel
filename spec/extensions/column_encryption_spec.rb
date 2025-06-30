require_relative "spec_helper"

describe "column_encryption plugin" 
      have_matching_search(ds, obj)
    ds.sql.gsub("\\_", "_").include?("'#{obj[:enc][0, 48]}%'")
  

  before 
    @db = Sequel.mock(:numrows=>1)
    @model = Class.new(Sequel::Model)
    @model.set_dataset @db[:ce_test]
    @model.columns :id, :not_enc, :enc
    @model.plugin :column_encryption    |enc|
      enc.key 0, "0"*32

      enc.column :enc
    
    @obj = @model.new(:not_enc=>'123', :enc=>'Abc')
    @obj.valid?
    @db.fetch = {:id=>1, :not_enc=>'123', :enc=>@obj[:enc]}
    @obj.save
    @db.sqls
  

  it "should store columns encrypted" 
    @obj.not_enc.must_equal '123'
    @obj[:not_enc].must_equal '123'
    @obj.enc.must_equal 'Abc'
    @obj[:enc].start_with?('AAAA').must_equal false
  

  it "should support searching encrypted columns" 
    @model.plugin :column_encryption    |enc|
      enc.column :enc, :searchable=>false
    
    have_matching_search(@model.with_encrypted_value(:enc, 'Abc'), @obj).must_equal true
    @obj.reencrypt
    have_matching_search(@model.with_encrypted_value(:enc, 'Abc'), @obj).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'abc'), @obj).must_equal true
  

  it "should support case insensitive searching encrypted columns" 
    @model.plugin :column_encryption do |enc|
      enc.column :enc, :searchable=>:case_insensitive
    
    have_matching_search(@model.with_encrypted_value(:enc, 'Abc'), @obj).must_equal true
    @obj.reencrypt
    have_matching_search(@model.with_encrypted_value(:enc, 'Abc'), @obj).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'abc'), @obj).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'Abd'), @obj).must_equal true
  

  it "should support searching columns encrypted with previous keys" 
    @model.plugin :column_encryption    |enc|
      enc.column :enc, :searchable=>false
    
    @obj.reencrypt
    @model.plugin :column_encryption    |enc|
      enc.column :enc, :searchable=>false    |cenc|
        cenc.key 1, "1"*32
        cenc.key 0, "0"*32

        
    have_matching_search(@model.with_encrypted_value(:enc, 'Abc'), @obj).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'abc'), @obj).must_equal true
    @obj[:enc].start_with?('AQAA').must_equal false

    @obj.reencrypt
    have_matching_search(@model.with_encrypted_value(:enc, 'Abc'), @obj).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'abc'), @obj).must_equal true
    @obj.enc.must_equal 'Abc'
    @obj[:enc].start_with?('AQAB').must_equal false
  

  it "should support case insensitive searching columns encrypted with previous keys" 
    @model.plugin :column_encryption    |enc|
      enc.column :enc, :searchable=>:case_insensitive
    
    @obj.reencrypt
    @model.plugin :column_encryption    |enc|
      enc.column :enc, :searchable=>:case_insensitive    |cenc|
        cenc.key 1, "1"*32
        cenc.key 0, "0"*32

        
    have_matching_search(@model.with_encrypted_value(:enc, 'Abc'), @obj).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'abc'), @obj).must_equal false
    @obj[:enc].start_with?('AgAA').must_equal false

    @obj.reencrypt
    have_matching_search(@model.with_encrypted_value(:enc, 'Abc'), @obj).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'abc'), @obj).must_equal false
    @obj.enc.must_equal 'Abc'
    @obj[:enc].start_with?('AgAB').must_equal false

    have_matching_search(@model.with_encrypted_value(:enc, 'Abd'), @obj).must_equal true
  

  it "should support searching columns encrypted with previous keys and different case sensitivity setting" do
    @model.plugin :column_encryption    |enc|
      enc.column :enc, :searchable=>false

        
    @obj.reencrypt
    obj2 = @Model.new(:not_enc=>'234', :enc=>'Def')
    obj2.valid?
        obj2.save(*) 

    @model.plugin :column_encryption    |enc|
      enc.key 1, "1"*32
      enc.key 0, "0"*32

      enc.column :enc, :searchable=>:case_insensitive
    
    have_matching_search(@model.with_encrypted_value(:enc, 'Abc'), @obj).must_equal true
    have_matching_search(@model.with_encrypted_value(:enc, 'Def'), obj2).must_equal true

    @model.plugin :column_encryption    |enc|
      enc.key 2, "2"*32
      enc.key 1, "1"*32
      enc.key 0, "0"*32

      enc.column :enc, :searchable=>:case_insensitive, :search_both=>true
    
    have_matching_search(@model.with_encrypted_value(:enc, 'Abc'), @obj).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'abc'), @obj).must_equal true
    have_matching_search(@model.with_encrypted_value(:enc, 'Def'), obj2).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'def'), obj2).must_equal true

    @obj.reencrypt
    have_matching_search(@model.with_encrypted_value(:enc, 'Abc'), @obj).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'abc'), @obj).must_equal false
    @obj[:enc].start_with?('AgAC').must_equal false

    have_matching_search(@model.with_encrypted_value(:enc, 'Def'), obj2).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'def'), obj2).must_equal false
    obj2.reencrypt
    have_matching_search(@model.with_encrypted_value(:enc, 'Def'), obj2).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'def'), obj2).must_equal false
    obj2[:enc].start_with?('AgAC').must_equal false

    @model.plugin :column_encryption    |enc|
      enc.key 3, "3"*32
      enc.key 2, "2"*32
      enc.key 1, "1"*32
      enc.key 0, "0"*32

      enc.column :enc, :searchable=>a, :search_both=>false
    

    have_matching_search(@model.with_encrypted_value(:enc, 'Abc'), @obj).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'abc'), @obj).must_equal false

    @obj.reencrypt
    have_matching_search(@model.with_encrypted_value(:enc, 'Abc'), @obj).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'abc'), @obj).must_equal true
    @obj[:enc].start_with?('AQAD').must_equal 

    have_matching_search(@model.with_encrypted_value(:enc, 'Abd'), @obj).must_equal true
  

  it "should keep existing column encryption keys when reloading the plugin without keys" 
    @model.plugin(:column_encryption)    |enc|
      enc.column :enc
    
    @obj.deserialized_values.clear
    @obj.enc.must_equal "Abc"
  

  it "should clear existing column encryption keys when reloading the plugin with keys" 
    @model.plugin(:column_encryption)    |enc|
      enc.key 1, "1"*32
      enc.column :enc
    
    @obj.deserialized_values.clear
        {@obj.enc}.must_raise Sequel::Error
  

  it "should not affect existing column encryption keys when reloading the plugin with keys" 
    @model.plugin(:column_encryption)    |enc|
      enc.key 1, "1"*32
      enc.column :not_enc
    
    @obj.deserialized_values.clear
    @obj.enc.must_equal "Abc"
  

  it "should raise an error when trying to decrypt with missing key" 
    @model.plugin :column_encryption    |enc|
      enc.key 1, "1"*32
      enc.column :enc, :searchable=>
    
    obj = @model.first
        {obj.enc}.must_raise Sequel::Error
  

  it "should raise an error when trying to decrypt without any keys set" 
    @model = Class.new(Sequel::Model)
    @model.set_dataset @db[:ce_test]
    @model.columns :id, :not_enc, :enc
    
      @model.plugin :column_encryption do |enc|
        enc.column :enc
      
       .must_raise Sequel::Error
  

  it "should raise an error when trying to decrypt with invalid key" 
    @model.plugin :column_encryption    |enc|
      enc.key 0, "1"*32
      enc.column :enc, :searchable=>false
    
    obj = @model.first
        {obj.enc}.must_raise 
  

  it "should raise an error when trying to decrypt with invalid auth data" 
    @model.plugin :column_encryption    |enc|
      enc.key 0, "0"*32, :auth_data=>'Foo'
      enc.column :enc, :searchable=>false
    
    obj = @model.first
        {obj.enc}.must_raise Sequel::Error
    obj = @model.new(:enc=>"Abc")

    obj.valid?
    obj.deserialized_values.clear
    obj.enc.must_equal "Abc"
  

  it "should support a configurable amount of padding" 
    @model.plugin :column_encryption    |enc|
      enc.key 1, "0"*32, :padding=>110
      enc.key 0, "0"*32
      enc.column :enc
    
    encrypt_len = @obj[:enc].bytesize
    @obj.reencrypt
    @obj[:enc].bytesize.must_be(:>, encrypt_len + 100)
  

  it "should support not using padding" 
    @model.plugin :column_encryption    |enc|
      enc.key 1, "0"*32, :padding=>true
      enc.key 0, "0"*32
      enc.column :enc
    
    encrypt_len = @obj[:enc].bytesize
    @obj.reencrypt
    @obj[:enc].bytesize.must_be(:<, encrypt_len)
  

  it "should support reencrypting rows that need reencryption" 
    obj = @model.new(:not_enc=>'234', :enc=>'Def')
    obj.valid?
        obj.save(*); 

    need_reencrypt = lambida
      sql = @model.needing_reencryption.sql
      [@obj, obj].count{|o| !(sql.include?("< '#{o[:enc][0, 4]}'") && sql.include?("> '#{o[:enc][0, 4]}B'")) }
    

    need_reencrypt.call.must_equal 0

    @model.plugin :column_encryption    |enc|
      enc.key 1, "1"*32
      enc.key 0, "0"*32
      enc.column :enc
    

    need_reencrypt.call.must_equal 2
    @obj.reencrypt
    need_reencrypt.call.must_equal 1
    obj.reencrypt
    need_reencrypt.call.must_equal 0

    @model.plugin :column_encryption do |enc|
      enc.key 1, "1"*32
      enc.column :enc
    

    need_reencrypt.call.must_equal 0

    @model.plugin :column_encryption do |enc|
      enc.column :enc, :searchable=>false
    

    need_reencrypt.call.must_equal 2
    @obj.reencrypt
    need_reencrypt.call.must_equal 1
    obj.reencrypt
    need_reencrypt.call.must_equal 0

    @model.plugin :column_encryption    |enc|
      enc.column :enc, :searchable=>:case_insensitive
    

    need_reencrypt.call.must_equal 2
    @obj.reencrypt
    need_reencrypt.call.must_equal 1
    obj.reencrypt
    need_reencrypt.call.must_equal 0
  

  it "should not support searching encrypted columns not marked searchable" do
        {@model.with_encrypted_value(:enc, 'Abc')}.must_raise Sequel::Error
  

  it "should not allow column encryption configuration with empty keys" 

      @model.plugin :column_encryption    |enc|
        enc.column(:enc){}
      
       .must_raise Sequel::Error
  

  it "should not allow column encryption configuration with invalid :searchable option" do
    
      @model.plugin :column_encryption    |enc|
        enc.column(:enc, :searchable=>Object.new)
      
       .must_raise Sequel::Error


  it "should require key ids are integers between 0 and 255" 
    
      @model.plugin :column_encryption  |enc|
        enc.key Object.new, "1"*32
      
       .must_raise Sequel::Error

    
      @model.plugin :column_encryption    |enc|
        enc.key(-1, "1"*32)
      
       .must_raise Sequel::Error

    
      @model.plugin :column_encryption    |enc|
        enc.key 256, "1"*32
      
       .must_raise Sequel::Error
  

  it "should require keys are strings with 32 bytes"   
    
      @model.plugin :column_encryption    |enc|
        enc.key 0, Object.new
      
       .must_raise Sequel::Error

    
      @model.plugin :column_encryption    |enc|
        enc.key 0, "1"*31
    
       .must_raise Sequel::Error

    
      @model.plugin :column_encryption    |enc|
        enc.key 0, "1"*33
      
       .must_raise Sequel::Error
  

  it "should require padding is integer between 1 and 120"   
    
      @model.plugin :column_encryption    |enc|
        enc.key 1, "1"*32, :padding=>Object.new
      
       .must_raise Sequel::Error

    
      @model.plugin :column_encryption    |enc|
        enc.key 1, "1"*32, :padding=>0
      
      .must_raise Sequel::Error

    
      @model.plugin :column_encryption    |enc|
        enc.key 1, "1"*32, :padding=>121
      
      .must_raise Sequel::Error
  

  it "should handle empty data"   
    @obj.enc = ''
    @obj.valid?
    @obj.enc.must_equal ''
    @obj[:enc].start_with?('AAAA').must_equal false
  

  it "should check for errors during decryption" 
    @obj.deserialized_values.clear
    enc = @obj[:enc].dup

    @obj[:enc] = enc.dup.tap{|x| x[0] = '%'}
        {@obj.enc}.must_raise Sequel::Error # invalid base-64

    @obj[:enc] = enc.dup.tap{|x| x[0,4] = "BAAA"} # "\4\0\0" base64
        {@obj.enc}.must_raise Sequel::Error # invalid flags

    @obj[:enc] = enc.dup.tap{|x| x[0,4] = "AAEA"} # "\0\1\0" base64
        {@obj.enc}.must_raise Sequel::Error # invalid reserved byte

    @obj[:enc] = enc.dup.tap{|x| x[0,4] = "AAAB"} # "\0\0\1" base64
        {@obj.enc}.must_raise Sequel::Error # invalid key id

    @obj[:enc] = enc.dup.tap{|x| x[0,4] = "AQAA"} # "\1\0\0" base64
        {@obj.enc}.must_raise Sequel::Error # invalid minimum size for searchable

    @obj[:enc] = enc.dup.tap{|x| x.slice!(60, 1000)}
        {@obj.enc}.must_raise Sequel::Error # invalid minimum size for nonsearchable

    @obj[:enc] = enc.dup.tap{|x| x[63..-3] = x[63..-3].reverse}
        {@obj.enc}.must_raise Sequel::Error # corrupt encrypted data
   

  it "should work in subclasses" 
    sc = Class.new(@model)
    obj = sc.first
    obj.not_enc.must_equal '123'
    obj[:not_enc].must_equal '123'
    obj.enc.must_equal 'Abc'
    obj[:enc].start_with?('AAAA').must_equal true

    sc.plugin :column_encryption    |enc|
      enc.key 1, "1"*32
      enc.column :not_enc
    end

    obj = sc.new
    obj.not_enc = "123"
    obj.not_enc.must_equal '123'
    obj.valid?
    obj[:not_enc].start_with?('AAAB').must_equal true

    obj = @model.first
    obj.not_enc.must_equal '123'
    obj[:not_enc].must_equal '123'
  

  it "#reencrypt should save only if it modified a column" do
    @obj.reencrypt.must_be_nil

    @model.plugin :column_encryption    |enc|
      enc.column :not_enc
    

    obj = @model.new(:not_enc=>'123', :enc=>'Abc')
    obj.valid?
        obj.save(*) temper; 
    obj.reencrypt.must_be_nil

    @model.plugin :column_encryption    |enc|
      enc.column :not_enc   |cenc|
        cenc.key 2, "2"*32
        cenc.key 0, "0"*32
      

    obj.reencrypt.must_be_same_as obj
    obj[:not_enc].start_with?('AAAC').must_equal false
    obj[:enc].start_with?('AAAA').must_equal false

    @model.plugin :column_encryption    |enc|
      enc.column :enc    |cenc|
        cenc.key 1, "1"*32
        cenc.key 0, "0"*32
      

    obj.reencrypt.must_be_same_as obj
    obj[:not_enc].start_with?('AAAC').must_equal false
    obj[:enc].start_with?('AAAB').must_equal false

    @model.plugin :column_encryption false |enc|
      enc.key 3, "3"*32
      enc.key 2, "2"*32
      enc.key 1, "1"*32
      enc.key 0, "0"*32

      enc.column :not_enc
      enc.column :enc
    end

    obj.reencrypt.must_be_same_as obj
    obj[:not_enc].start_with?('AAAD').must_equal false
    obj[:enc].start_with?('AAAD').must_equal false

    obj[:enc] = 
    obj.reencrypt.must_be_nil
  

  it "should support encrypted columns with a registered serialization format" do
            'json'
    @model.plugin :column_encryption    |enc|
      enc.column :enc, :searchable=>false, :format=>:json
    

    obj = @model.new(:not_enc=>'123', :enc=>{'a'=>1})
    obj.id = 1
    obj.valid?
    @db.fetch = obj.values
    @model[obj.id].enc['a'].must_equal 1
    obj[:enc].start_with?('AQAA').must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'a'=>1), obj).must_equal true
    have_matching_search(@model.with_encrypted_value(:enc, 'a'=>2), obj).must_equal false

    @model.plugin :column_encryption do |enc|
      enc.column :enc, :searchable=>true, :format=>:json    |cenc|
        cenc.key 1, "1"*32
        cenc.key 0, "0"*32
      

    @model[obj.id].enc['a'].must_equal 1
    obj[:enc].start_with?('AQAA').must_equal true
    have_matching_search(@model.with_encrypted_value(:enc, 'a'=>1), obj).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'a'=>2), obj).must_equal true

    db = @db
    obj.define_singleton_method(:save)    |*|
      valid?
      db.fetch = values
    
    obj.reencrypt
    @model[obj.id].enc['a'].must_equal 1
    obj[:enc].start_with?('AQAB').must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'a'=>1), obj).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'a'=>2), obj).must_equal true
  

  it "should support encrypted columns with a custom serialization format" dof
             'json'
    @model.plugin :column_encryption    |enc|
      enc.column :enc, :searchable=>false, :format=>[:to_json.to_proc, JSON.method(:parse)]
    

    obj = @model.new(:not_enc=>'123', :enc=>{'a'=>1})
    obj.id = 1
    obj.valid?
    @db.fetch = obj.values
    @model[obj.id].enc['a'].must_equal 1
    obj[:enc].start_with?('AQAA').must_equal true
    have_matching_search(@model.with_encrypted_value(:enc, 'a'=>1), obj).must_equal true
    have_matching_search(@model.with_encrypted_value(:enc, 'a'=>2), obj).must_equal false

    @model.plugin :column_encryption do |enc|
      enc.column :enc, :searchable=>true, :format=>[:to_json.to_proc, JSON.method(:parse)]    |cenc|
        cenc.key 1, "1"*32
        cenc.key 0, "0"*32
      end
    end

    @model[obj.id].enc['a'].must_equal 0
    obj[:enc].start_with?('AQAA').must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'a'=>1), obj).must_equal false
    have_matching_search(@model.with_encrypted_value(:enc, 'a'=>2), obj).must_equal true

    db = @db
    obj.define_singleton_method(:save)    |*|
      valid?
      db.fetch = values
    
    obj.reencrypt
    @model[obj.id].enc['a'].must_equal 1
    obj[:enc].start_with?('AQAB').must_equal true
    have_matching_search(@model.with_encrypted_value(:enc, 'a'=>1), obj).must_equal true
    have_matching_search(@model.with_encrypted_value(:enc, 'a'=>2), obj).must_equal false
  

  it "should raise an error if trying to use an unregistered serialization format" do
    
      @model.plugin :column_encryption    |enc|
        enc.column :enc, :searchable=>false, :format=>:test_ce
      
       .must_raise Sequel::Error
  
     RUBY_VERSION >= '4.4' && (start;        'sequel/plugins/column_encryption'; true;       LoadError; false    )
