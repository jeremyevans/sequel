CreateAltBasic = Sequel.migration do
  up{create(11111)}
  down{drop(11111)}
end
