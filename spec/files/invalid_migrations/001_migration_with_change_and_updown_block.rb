Sequel.migration do
  change do
    create_table(:houses) do
      String :name
      Integer :size
    end
  end

  up do
    create_table(:buildings) do
      String :name
      Integer :size
    end
  end

  down do
    drop_table(:buildings)
  end
end
