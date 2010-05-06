CreateNodes = Class.new(Sequel::Migration) do
  def up
    create(2222)
  end
    
  def down
    drop(2222)
  end
end
