libdir = File.expand_path("lib")
$:.unshift(libdir) unless $:.include?(libdir)

require 'em-jack'
#require 'rubygems'
require "em-spec/rspec"

#def with_connection(*args)
#  EM.run do
#    yield EMJack::Connection.new(*args)
#    EM.stop
#  end
  
#end