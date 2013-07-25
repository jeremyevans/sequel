require "rake"
require "rake/clean"

NAME = 'sequel'
VERS = lambda do
  require File.expand_path("../lib/sequel/version", __FILE__)
  Sequel.version
end
CLEAN.include ["**/.*.sw?", "sequel-*.gem", ".config", "rdoc", "coverage", "www/public/*.html", "www/public/rdoc*", '**/*.rbc']
SUDO = ENV['SUDO'] || 'sudo'

# Gem Packaging and Release

desc "Build sequel gem"
task :package=>[:clean] do |p|
  sh %{#{FileUtils::RUBY} -S gem build sequel.gemspec}
end

desc "Install sequel gem"
task :install=>[:package] do
  sh %{#{SUDO} #{FileUtils::RUBY} -S gem install ./#{NAME}-#{VERS.call} --local}
end

desc "Uninstall sequel gem"
task :uninstall=>[:clean] do
  sh %{#{SUDO} #{FileUtils::RUBY} -S gem uninstall #{NAME}}
end

desc "Publish sequel gem to rubygems.org"
task :release=>[:package] do
  sh %{#{FileUtils::RUBY} -S gem push ./#{NAME}-#{VERS.call}.gem}
end

### Website

desc "Make local version of website"
task :website do
  sh %{#{FileUtils::RUBY} www/make_www.rb}
end

desc "Update Non-RDoc section of sequel.rubyforge.org"
task :website_rf_base=>[:website] do
  sh %{rsync -rt www/public/*.html rubyforge.org:/var/www/gforge-projects/sequel/}
end

### RDoc

RDOC_DEFAULT_OPTS = ["--line-numbers", "--inline-source", '--title', 'Sequel: The Database Toolkit for Ruby']

allow_website_rdoc = begin
  # Sequel uses hanna-nouveau for the website RDoc.
  # Due to bugs in older versions of RDoc, and the
  # fact that hanna-nouveau does not support RDoc 4,
  # a specific version of rdoc is required.
  gem 'rdoc', '= 3.12.2'
  gem 'hanna-nouveau'
  RDOC_DEFAULT_OPTS.concat(['-f', 'hanna'])
  true
rescue Gem::LoadError
  false
end

rdoc_task_class = begin
  require "rdoc/task"
  RDoc::Task
rescue LoadError
  begin
    require "rake/rdoctask"
    Rake::RDocTask
  rescue LoadError, StandardError
  end
end

if rdoc_task_class
  RDOC_OPTS = RDOC_DEFAULT_OPTS + ['--main', 'README.rdoc']

  rdoc_task_class.new do |rdoc|
    rdoc.rdoc_dir = "rdoc"
    rdoc.options += RDOC_OPTS
    rdoc.rdoc_files.add %w"README.rdoc CHANGELOG MIT-LICENSE lib/**/*.rb doc/*.rdoc doc/release_notes/*.txt"
  end

  if allow_website_rdoc
    desc "Make rdoc for website"
    task :website_rdoc=>[:website_rdoc_main, :website_rdoc_adapters, :website_rdoc_plugins]
  
    rdoc_task_class.new(:website_rdoc_main) do |rdoc|
      rdoc.rdoc_dir = "www/public/rdoc"
      rdoc.options += RDOC_OPTS + %w'--no-ignore-invalid'
      rdoc.rdoc_files.add %w"README.rdoc CHANGELOG MIT-LICENSE lib/*.rb lib/sequel/*.rb lib/sequel/{connection_pool,dataset,database,model}/*.rb doc/*.rdoc doc/release_notes/*.txt lib/sequel/extensions/migration.rb lib/sequel/extensions/core_extensions.rb"
    end
  
    rdoc_task_class.new(:website_rdoc_adapters) do |rdoc|
      rdoc.rdoc_dir = "www/public/rdoc-adapters"
      rdoc.options += RDOC_DEFAULT_OPTS + %w'--main Sequel --no-ignore-invalid'
      rdoc.rdoc_files.add %w"lib/sequel/adapters/**/*.rb"
    end
  
    rdoc_task_class.new(:website_rdoc_plugins) do |rdoc|
      rdoc.rdoc_dir = "www/public/rdoc-plugins"
      rdoc.options += RDOC_DEFAULT_OPTS + %w'--main Sequel --no-ignore-invalid'
      rdoc.rdoc_files.add %w"lib/sequel/{extensions,plugins}/**/*.rb"
    end
  
    desc "Update sequel.rubyforge.org"
    task :website_rf=>[:website, :website_rdoc] do
      sh %{rsync -rvt www/public/* rubyforge.org:/var/www/gforge-projects/sequel/}
    end
  end
end

### Specs

begin
  begin
    # RSpec 1
    require "spec/rake/spectask"
    spec_class = Spec::Rake::SpecTask
    spec_files_meth = :spec_files=
    spec_opts_meth = :spec_opts=
  rescue LoadError
    # RSpec 2
    require "rspec/core/rake_task"
    spec_class = RSpec::Core::RakeTask
    spec_files_meth = :pattern=
    spec_opts_meth = :rspec_opts=
  end

  spec = lambda do |name, files, d|
    lib_dir = File.join(File.dirname(File.expand_path(__FILE__)), 'lib')
    ENV['RUBYLIB'] ? (ENV['RUBYLIB'] += ":#{lib_dir}") : (ENV['RUBYLIB'] = lib_dir)

    desc "#{d} with -w, some warnings filtered"
    task "#{name}_w" do
      ENV['RUBYOPT'] ? (ENV['RUBYOPT'] += " -w") : (ENV['RUBYOPT'] = '-w')
      sh "#{FileUtils::RUBY} -S rake #{name} 2>&1 | egrep -v \"(spec/.*: warning: (possibly )?useless use of == in void context|: warning: instance variable @.* not initialized|: warning: method redefined; discarding old|: warning: previous definition of)|rspec\""
    end

    desc d
    spec_class.new(name) do |t|
      t.send spec_files_meth, files
      t.send spec_opts_meth, ENV['SEQUEL_SPEC_OPTS'].split if ENV['SEQUEL_SPEC_OPTS']
    end
  end

  spec_with_cov = lambda do |name, files, d, &b|
    spec.call(name, files, d)
    if RUBY_VERSION < '1.9'
      t = spec.call("#{name}_cov", files, "#{d} with coverage")
      t.rcov = true
      t.rcov_opts = File.file?("spec/rcov.opts") ? File.read("spec/rcov.opts").split("\n") : []
      b.call(t) if b
    else
      desc "#{d} with coverage"
      task "#{name}_cov" do
        ENV['COVERAGE'] = '1'
        Rake::Task[name].invoke
      end
    end
    t
  end

  task :default do
    STDERR.puts """
      Running core, model, bin, plugin and core_ext specs.
      Run other specs manually with rake {spec_integration,spec_sqlite,spec_postgres,spec_mysql}.
    """
    [:spec, :spec_bin, :spec_plugin, :spec_core_ext].each {|t| Rake::Task[t].invoke }
  end

  spec_with_cov.call("spec", Dir["spec/{core,model}/*_spec.rb"], "Run core and model specs"){|t| t.rcov_opts.concat(%w'--exclude "lib/sequel/(adapters/([a-ln-z]|m[a-np-z])|extensions/core_extensions)"')}
  spec.call("spec_bin", ["spec/bin_spec.rb"], "Run bin/sequel specs")
  spec.call("spec_core", Dir["spec/core/*_spec.rb"], "Run core specs")
  spec.call("spec_model", Dir["spec/model/*_spec.rb"], "Run model specs")
  spec.call("_spec_model_no_assoc", Dir["spec/model/*_spec.rb"].delete_if{|f| f =~ /association|eager_loading/}, '')
  spec_with_cov.call("spec_core_ext", ["spec/core_extensions_spec.rb"], "Run core extensions specs"){|t| t.rcov_opts.concat(%w'--exclude "lib/sequel/([a-z_]+\.rb|adapters|connection_pool|database|dataset|model)"')}
  spec_with_cov.call("spec_plugin", Dir["spec/extensions/*_spec.rb"].sort_by{rand}, "Run extension/plugin specs"){|t| t.rcov_opts.concat(%w'--exclude "lib/sequel/([a-z_]+\.rb|adapters|connection_pool|database|dataset|model)"')}
  spec_with_cov.call("spec_integration", Dir["spec/integration/*_test.rb"], "Run integration tests")

  %w'postgres sqlite mysql informix oracle firebird mssql db2'.each do |adapter|
    spec_with_cov.call("spec_#{adapter}", ["spec/adapters/#{adapter}_spec.rb"] + Dir["spec/integration/*_test.rb"], "Run #{adapter} specs"){|t| t.rcov_opts.concat(%w'--exclude "lib/sequel/([a-z_]+\.rb|connection_pool|database|dataset|model|extensions|plugins)"')}
  end

  task :spec_travis=>[:spec, :spec_plugin, :spec_core_ext] do
    if defined?(RUBY_ENGINE) && RUBY_ENGINE == 'jruby'
      ENV['SEQUEL_SQLITE_URL'] = "jdbc:sqlite::memory:"
      ENV['SEQUEL_POSTGRES_URL'] = "jdbc:postgresql://localhost/sequel_test?user=postgres"
      ENV['SEQUEL_MYSQL_URL'] = "jdbc:mysql://localhost/sequel_test?user=root"
    else
      ENV['SEQUEL_SQLITE_URL'] = "sqlite:/"
      ENV['SEQUEL_POSTGRES_URL'] = "postgres://localhost/sequel_test?user=postgres"
      ENV['SEQUEL_MYSQL_URL'] = "mysql2://localhost/sequel_test?user=root"
    end

    Rake::Task['spec_sqlite'].invoke
    Rake::Task['spec_postgres'].invoke
    Rake::Task['spec_mysql'].invoke
  end

  desc "Run model specs without the associations code"
  task :spec_model_no_assoc do
    ENV['SEQUEL_NO_ASSOCIATIONS'] = '1'
    Rake::Task['_spec_model_no_assoc'].invoke
  end
rescue LoadError
  task :default do
    puts "Must install rspec to run the default task (which runs specs)"
  end
end

desc "Print Sequel version"
task :version do
  puts VERS.call
end

desc "Check syntax of all .rb files"
task :check_syntax do
  Dir['**/*.rb'].each{|file| print `#{FileUtils::RUBY} -c #{file} | fgrep -v "Syntax OK"`}
end
