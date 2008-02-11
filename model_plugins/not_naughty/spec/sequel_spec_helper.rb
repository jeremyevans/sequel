require File.dirname(__FILE__) + '/spec_helper.rb'
require 'sequel_not_naughty'

DB = Sequel::Model.db = Sequel.sqlite

Sequel::Model.instance_eval do
  %w[validate valid?].
  each {|m| undef_method m.to_sym}
end

(class << Sequel::Model; self; end).module_eval do
  %w[validate validates validates_acceptance_of validates_confirmation_of
     validates_each validates_format_of validates_length_of 
     validates_numericality_of validates_presence_of validations
     has_validations?].
  each {|m| undef_method m.to_sym}
end
