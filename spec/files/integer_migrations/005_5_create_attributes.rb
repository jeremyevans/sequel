CreateAttributes = Sequel.migration do
  up{create_table(:sm5555){Integer :smc5}}
  down{drop_table(:sm5555)}
end
