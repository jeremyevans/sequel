# Dataset methods are methods that the model class extends its dataset with in
# the call to set_dataset.
module Sequel::Model::DatasetMethods
  attr_accessor :model

  # Destroy each row in the dataset by instantiating it and then calling
  # destroy on the resulting model object.  This isn't as fast as deleting
  # the object, which does a single SQL call, but this runs any destroy
  # hooks.
  def destroy
    count = 0
    @db.transaction{all{|r| count += 1; r.destroy}}
    count
  end

  # This allows you to call to_hash without any arguments, which will
  # result in a hash with the primary key value being the key and the
  # model object being the value.
  def to_hash(key_column=nil, value_column=nil)
    if key_column
      super
    else
      raise(Sequel::Error, "No primary key for model") unless model and pk = model.primary_key
      super(pk, value_column) 
    end
  end
end
