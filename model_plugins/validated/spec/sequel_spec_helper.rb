require "#{ File.dirname(__FILE__) }/spec_helper.rb"
require 'sequel_validated'

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

def sequel() ::Sequel::Plugins::Validated end

__END__
require 'digest/md5'
module Foo
  def self.match(address)
    address[/[^@]+@([^\.@]+)+(\.[^\.@]+)+/]
  end
end

class Login < Sequel::Model
  is :validated

  validates(:username) { presence and length :within => 6..48 }
  validates(:password, :if => :password?) { confirmation; length :minimum => 8 }

  validates do
    presence_of :password_hash, :password_salt, :after => :password_hashed
  end

  validates_format_of :email, :with => Foo

  protected
  def password_hashed
    if valid? password
      self.password_salt = Digest::MD5.
        hex_digest(Time.now.to_s)[0, 16]
      self.password_hash = Digest::MD5.
        hex_digest(password + password_salt)
    end
  end
  def password_set?
    !password.nil?
  end
end
