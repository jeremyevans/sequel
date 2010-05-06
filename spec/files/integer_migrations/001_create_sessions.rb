class CreateSessions < Sequel::Migration
  def up
    create(1111)
  end
  
  def down
    drop(1111)
  end
end
