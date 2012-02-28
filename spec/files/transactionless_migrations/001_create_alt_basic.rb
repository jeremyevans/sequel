Sequel.migration do
  no_transaction
  change{create_table(:sm11111){Integer :smc1}}
end
