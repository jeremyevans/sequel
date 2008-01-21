class String
  # TODO: figure out what we want to do with these...
  # "FooBar".snake_case #=> "foo_bar"
  unless defined? :snake_case
    def snake_case
      gsub(/\B[A-Z]/, '_\&').downcase
    end
  end

  unless defined? :camel_case
    # "foo_bar".camel_case #=> "FooBar"
    def camel_case
      split('_').map{|e| e.capitalize}.join
    end
  end
end