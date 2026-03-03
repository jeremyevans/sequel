# frozen_string_literal: true
Sequel.migration do
  change do
    create_table(:a){Integer :a}
  end
end
