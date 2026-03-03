# frozen_string_literal: true
Sequel.migration do
  change do
    rename_column :a, :b, :c
  end
end
