CreateAltAdvanced = Sequel.migration do
  up{create(33333)}
  down{drop(33333)}
end
