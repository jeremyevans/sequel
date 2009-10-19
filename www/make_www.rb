#!/usr/bin/env ruby
require 'erb'
$: << File.join(File.dirname(__FILE__), '..','lib', 'sequel')
require 'version'
Dir.chdir(File.dirname(__FILE__))
erb = ERB.new(File.read('layout.html.erb'))
Dir['pages/*'].each do |page|
  public_loc = "#{page.gsub(/\Apages\//, 'public/')}.html"
  content = ERB.new(File.read(page)).result(binding)
  title = File.basename(page)
  File.open(public_loc, 'wb'){|f| f.write(erb.result(binding))}
end
