CreateUsers = Sequel.migration do
  up{create(3333)}
  down{drop(3333)}
end
