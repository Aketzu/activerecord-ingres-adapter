# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)

Gem::Specification.new do |s|
  s.name = "activerecord-ingres-adapter"
  s.version = "1.4.16"
  s.summary = "ActiveRecord Ingres Adapter"
  s.email = "felix.bellanger@gmail.com"
  s.homepage = "https://github.com/Keeguon/activerecord-ingres-adapter"
  s.description = "ActiveRecord Ingres Adapter and driver for the Ingres Enterprise Relational Database"
  s.authors = ["Grant Croker","Bruce Lunsford","Jared Richardson","Felix Bellanger"]

  s.rubyforge_project = "activerecord-ingres-adapter"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- test/*`.split("\n")
  s.require_paths = ["lib"]
  s.extensions    = ['ext/extconf.rb']

  #s.add_dependency("arel", "~> 3.0.2")
end
