class TUA < Sequel::Model
  many_to_one :a1, :key=>:t_id, :class=>self
  one_to_many :a2s, :key=>:t_id, :class=>self
  one_to_one :a3, :key=>:t_id, :class=>self
  a4s_opts = {:right_key=>:t_id, :left_key=>:t_id, :class=>self}
  include(a4s_opts[:methods_module] = Module.new) if ENV['A4S_METHODS_MODULE']
  a4s_opts[:read_only] = true if ENV['A4S_READ_ONLY']
  if ENV['A4S_NO_METHODS']
    a4s_opts[:no_dataset_method] = a4s_opts[:no_association_method] = true
    a4s_opts[:adder] = a4s_opts[:remover] = a4s_opts[:clearer] = nil
  end
  many_to_many :a4s, a4s_opts
  one_through_one :a5, :right_key=>:t_id, :left_key=>:t_id, :class=>self, :is_used=>!!ENV['A5_IS_USED']
  one_to_many :a6s, :key=>:t_id, :class=>self, :is_used=>!!ENV['A6S_IS_USED']

  O = load(:id=>1, :t_id=>2)

  class SC < self
    many_to_one :a7, :key=>:t_id, :class=>self

    O = load(:id=>1, :t_id=>2)
  end
end

# Class with no associations
class TUA2 < Sequel::Model
end

# Anonymous class with associations
Class.new(Sequel::Model(DB[:tuas])) do
  many_to_one :a1, :key=>:t_id, :class=>self
end
