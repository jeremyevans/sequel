# frozen_string_literal: true
Sequel.migration do
  change do
    rename_table :a, :b
  end
end
