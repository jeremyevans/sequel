# frozen_string_literal: true
Sequel.migration do
  transaction
  change{create_table(:sm11111){Integer :smc1}}
end
