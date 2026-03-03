# frozen_string_literal: true
Sequel.migration do
  change{create_table(:sm){Integer :smc1}}
end
