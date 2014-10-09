require 'rubygems'
require 'rubygems/package_task'
require 'rdoc/task'
require 'rake/testtask'

spec = Gem::Specification.new do |s|
  s.authors = ["Grant Croker","Bruce Lunsford","Jared Richardson"]
  s.email = "grant.croker@ingres.com"
  s.name = "activerecord-ingres-adapter"
  s.version = "1.4.5"
  s.date = Time.now.localtime.strftime("%Y-%m-%d")
  s.summary = "ActiveRecord Ingres Adapter"
  s.description = "ActiveRecord Ingres Adapter and driver for the Ingres Enterprise Relational Database"
  s.require_path = "lib"
  s.homepage = "http://community.ingres.com/wiki/Ruby"
  s.rubyforge_project = "activerecord-ingres-adapter"
  s.extensions << 'ext/extconf.rb'
  s.has_rdoc = true
  s.extra_rdoc_files = ['README.rdoc','CHANGELOG', 'ext/Ingres.c']
  s.rdoc_options << '--title' << 'Fb -- Ingres Ruby Driver and ActiveRecord Adapter' << '-x' << 'test'
  s.files = Dir['ext/**/*', 'lib/**/*','lib/active_record/connection_adapters/*','CHANGELOG','LICENSE.txt','TODO.txt', 'Rakefile','cookbook_ingres.sql']
end

Gem::PackageTask.new(spec) do |pkg|
  pkg.need_tar = true
  pkg.need_zip = true
end

Rake::TestTask.new do |t|
  t.libs << "ext/tests"
  t.test_files = FileList['ext/tests/ts_*.rb','ext/tests/tc_*.rb']
  t.verbose = true
end
