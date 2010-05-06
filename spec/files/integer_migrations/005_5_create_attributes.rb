CreateAttributes = Sequel.migration do
  up{create(5555)}
  down{drop(5555)}
end
