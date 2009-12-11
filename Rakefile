libdir = File.expand_path("lib")
$:.unshift(libdir) unless $:.include?(libdir)

begin
  require 'jeweler'
  Jeweler::Tasks.new do |s|
    s.name = "em-beanstalk"
    s.description = s.summary = "EventMachine client for Beanstalkd"
    s.email = "dan@postrank.com"
    s.homepage = "http://github.com/joshbuddy/em-beastalk"
    s.authors = ["Dan"]
    s.files = FileList["[A-Z]*", "{lib,spec}/**/*"]
    s.add_dependency 'eventmachine'
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler not available. Install it with: sudo gem install technicalpickles-jeweler -s http://gems.github.com"
end

require 'spec'
require 'spec/rake/spectask'
task :spec => 'spec:all'
namespace(:spec) do
  Spec::Rake::SpecTask.new(:all) do |t|
    t.spec_opts ||= []
    t.spec_opts << "-rubygems"
    t.spec_opts << "--options" << "spec/spec.opts"
    t.spec_files = FileList['spec/**/*_spec.rb']
  end

end

desc "Run all examples with RCov"
Spec::Rake::SpecTask.new('spec_with_rcov') do |t|
  t.spec_files = FileList['spec/**/*.rb']
  t.rcov = true
  t.rcov_opts = ['--exclude', 'spec']
end

require 'rake/rdoctask'
desc "Generate documentation"
Rake::RDocTask.new do |rd|
  rd.main = "README.rdoc"
  rd.rdoc_files.include("README.rdoc", "lib/**/*.rb")
  rd.rdoc_dir = 'rdoc'
end
