class CreateSessions < Sequel::Migration
  def up
    create_table(:sm1111){Integer :smc1}
  end

  def down
    get(:asdfsadfas)
  end
end
