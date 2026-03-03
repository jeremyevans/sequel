# frozen_string_literal: true
Sequel.migration do
  change do
    add_column :a, :b, String
  end
end
