# frozen-string-literal: true

# :nocov:

# This entire file is excluded from coverage testing.  This is because it
# requires coverage testing to work, and if you've already loaded Sequel
# without enabling coverage, then coverage testing won't work correctly
# for methods defined by Sequel.
#
# While automated coverage testing is disabled, manual coverage testing
# was used during spec development to make sure this code is 100% covered.

if RUBY_VERSION < '2.5'
  raise LoadError, "The Sequel unused_associations plugin depends on Ruby 2.5+ method coverage"
end

require 'coverage'
require 'json'

module Sequel
  module Plugins
    # The unused_associations plugin detects which model associations are not
    # used and can be removed, and which model association methods are not used
    # and can skip being defined. The advantage of removing unused associations
    # and unused association methods is decreased memory usage, since each
    # method defined takes memory and adds more work for the garbage collector.
    #
    # In order to detect which associations are used, this relies on the method
    # coverage support added in Ruby 2.5. To allow flexibility to override
    # association methods, the association methods that Sequel defines are
    # defined in a module included in the class instead of directly in the
    # class.  Unfortunately, that makes it difficult to directly use the
    # coverage data to find unused associations. The advantage of this plugin
    # is that it is able to figure out from the coverage information whether
    # the association methods Sequel defines are actually used.
    #
    # = Basic Usage
    #
    # The expected usage of the unused_associations plugin is to load it
    # into the base class for models in your application, which will often
    # be Sequel::Model:
    # 
    #   Sequel::Model.plugin :unused_associations
    #
    # Then you run your test suite with method coverage enabled, passing the
    # coverage result to +update_associations_coverage+.
    # +update_associations_coverage+ returns a data structure containing
    # method coverage information for all subclasses of the base class.
    # You can pass the coverage information to
    # +update_unused_associations_data+, which will return a data structure
    # with information on unused associations.
    #
    #   require 'coverage'
    #   Coverage.start(methods: true)
    #   # load sequel after starting coverage, then run your tests
    #   cov_data = Sequel::Model.update_associations_coverage
    #   unused_associations_data = Sequel::Model.update_unused_associations_data(coverage_data: cov_data)
    #
    # You can take that unused association data and pass it to the
    # +unused_associations+ method to get a array of information on
    # associations which have not been used.  Each entry in the array
    # will contain a class name and association name for each unused
    # association, both as a string:
    #
    #   Sequel::Model.unused_associations(unused_associations_data: unused_associations_data)
    #   # => [["Class1", "assoc1"], ...]
    #
    # You can use the output of the +unused_associations+ method to determine
    # which associations are not used at all in your application, and can
    # be eliminiated.
    #
    # You can also take that unused association data and pass it to the
    # +unused_association_options+ method, which will return an array of
    # information on associations which are used, but have related methods
    # defined that are not used. The first two entries in each array are
    # the class name and association name as a string, and the third
    # entry is a hash of association options:
    #
    #   Sequel::Model.unused_association_options(unused_associations_data: unused_associations_data)
    #   # => [["Class2", "assoc2", {:read_only=>true}], ...]
    #
    # You can use the output of the +unused_association_options+ to
    # find out which association options can be provided when defining
    # the association so that the association method will not define
    # methods that are not used.
    #
    # = Combining Coverage Results
    #
    # It is common to want to combine results from multiple separate
    # coverage runs.  For example, if you have multiple test suites
    # for your application, one for model or unit tests and one for
    # web or integration tests, you would want to combine the
    # coverage information from all test suites before determining
    # that the associations are not used.
    #
    # The unused_associations plugin supports combining multiple
    # coverage results using the :coverage_file plugin option:
    #
    #   Sequel::Model.plugin :unused_associations,
    #     coverage_file: 'unused_associations_coverage.json'
    #
    # With the coverage file option, +update_associations_coverage+
    # will look in the given file for existing coverage information,
    # if it exists.  If the file exists, the data from it will be
    # merged with the coverage result passed to the method.
    # Before returning, the coverage file will be updated with the
    # merged result.  When using the :coverage_file plugin option,
    # you can each of your test suites update the coverage
    # information:
    #
    #   require 'coverage'
    #   Coverage.start(methods: true)
    #   # run this test suite
    #   Sequel::Model.update_associations_coverage
    #
    # After all test suites have been run, you can run
    # +update_unused_associations_data+, without an argument:
    #
    #   unused_associations_data = Sequel::Model.update_unused_associations_data
    #
    # With no argument, +update_unused_associations_data+ will get
    # the coverage data from the coverage file, and then use that
    # to prepare the information.  You can then use the returned
    # value the same as before to get the data on unused associations.
    # To prevent stale coverage information, calling
    # +update_unused_associations_data+ when using the :coverage_file
    # plugin option will remove the coverage file by default (you can
    # use the :keep_coverage option to prevent the deletion of the
    # coverage file).
    #
    # = Automatic Usage of Unused Association Data
    #
    # Since it can be a pain to manually update all of your code
    # to remove unused assocations or add options to prevent the
    # definition of unused associations, the unused_associations
    # plugin comes with support to take previously saved unused
    # association data, and use it to not create unused associations,
    # and to automatically use the appropriate options so that unused
    # association methods are not created.
    #
    # To use this option, you first need to save the unused association
    # data previously prepared.  You can do this by passing an
    # :file option when loading the plugin.  
    #
    #   Sequel::Model.plugin :unused_associations,
    #     file: 'unused_associations.json'
    #
    # With the :file option provided, you no longer need to use
    # the return value of +update_unused_associations_data+, as
    # the file will be updated with the information:
    #
    #   Sequel::Model.update_unused_associations_data(coverage_data: cov_data)
    #
    # Then, to use the saved unused associations data, add the
    # :modify_associations plugin option:
    #
    #   Sequel::Model.plugin :unused_associations,
    #     file: 'unused_associations.json',
    #     modify_associations: true
    #
    # With the :modify_associations used, and the unused association
    # data file is available, when subclasses attempt to create an
    # unused association, the attempt will be ignored.  If the
    # subclasses attempt to create an association where not
    # all association methods are used, the plugin will automatically
    # set the appropriate options so that the unused association
    # methods are not defined.
    #
    # When you are testing which associations are used, make sure
    # not to set the :modify_associations plugin option, or make sure
    # that the unused associations data file does not exist.
    #
    # == Automatic Usage with Combined Coverage Results
    #
    # If you have multiple test suites and want to automatically
    # use the unused association data, you should provide both
    # :file and :coverage_file options when loading the plugin:
    #
    #   Sequel::Model.plugin :unused_associations,
    #     file: 'unused_associations.json',
    #     coverage_file: 'unused_associations_coverage.json'
    #
    # Then each test suite just needs to run
    # +update_associations_coverage+ to update the coverage information:
    #
    #   Sequel::Model.update_associations_coverage
    #
    # After all test suites have been run, you can run
    # +update_unused_associations_data+ to update the unused
    # association data file (and remove the coverage file):
    #
    #   Sequel::Model.update_unused_associations_data
    #
    # Then you can add the :modify_associations plugin option to
    # automatically use the unused association data.
    #
    # = Caveats
    #
    # Since this plugin is based on coverage information, if you do
    # not have tests that cover all usage of associations in your
    # application, you can end up with coverage that shows the
    # association is not used, when it is used in code that is not
    # covered.  The output of plugin can still be useful in such cases,
    # as long as you are manually checking it.  However, you should
    # avoid using the :modify_associations unless you have
    # confidence that your tests cover all usage of associations
    # in your application. You can specify the :is_used association
    # option for any association that you know is used. If an
    # association uses the :is_used association option, this plugin
    # will not modify it if the :modify_associations option is used.
    #
    # This plugin does not handle anonymous classes. Any unused
    # associations defined in anonymous classes will not be
    # reported by this plugin.
    #
    # This plugin only considers the public instance methods the
    # association defines, and direct access to the related
    # association reflection via Sequel::Model.association_reflection
    # to determine if the association was used.  If the association
    # metadata was accessed another way, it's possible this plugin
    # will show the association as unused.
    #
    # As this relies on the method coverage added in Ruby 2.5, it does
    # not work on older versions of Ruby.  It also does not work on
    # JRuby, as JRuby does not implement method coverage.
    module UnusedAssociations
      # Load the subclasses plugin, as the unused associations plugin
      # is designed to handle all subclasses of the class it is loaded
      # into.
      def self.apply(mod, opts=OPTS)
        mod.plugin :subclasses
      end

      # Plugin options:
      # :coverage_file :: The file to store the coverage information,
      #                   when combining coverage information from
      #                   multiple test suites.
      # :file :: The file to store and/or load the unused associations data.
      # :modify_associations :: Whether to use the unused associations data
      #                         to skip defining associations or association
      #                         methods.
      # :unused_associations_data :: The unused associations data to use if the
      #                              :modify_associations is used (by default, the
      #                              :modify_associations option will use the data from
      #                              the file specified by the :file option). This is
      #                              same data returned by the
      #                              +update_unused_associations_data+ method.
      def self.configure(mod, opts=OPTS)
        mod.instance_exec do
          @unused_associations_coverage_file = opts[:coverage_file]
          @unused_associations_file = opts[:file]
          @unused_associations_data = if opts[:modify_associations]
            if opts[:unused_associations_data]
              opts[:unused_associations_data]
            elsif File.file?(opts[:file])
              Sequel.parse_json(File.binread(opts[:file]))
            end
          end
        end
      end

      module ClassMethods
        # Only the data is copied to subclasses, to allow the :modify_associations
        # plugin option to affect them.  The :file and :coverage_file are not copied
        # to subclasses, as users are expected ot call methods such as
        # unused_associations only on the class that is loading the plugin.
        Plugins.inherited_instance_variables(self, :@unused_associations_data=>nil)

        # Synchronize access to the used association reflections.
        def used_association_reflections
          Sequel.synchronize{@used_association_reflections ||= {}}
        end

        # Record access to association reflections to determine which associations are not used.
        def association_reflection(association)
          uar = used_association_reflections
          Sequel.synchronize{uar[association] ||= true}
          super
        end

        # If modifying associations, and this association is marked as not used,
        # and the association does not include the specific :is_used option,
        # skip defining the association.
        def associate(type, assoc_name, opts=OPTS)
          if !opts[:is_used] && @unused_associations_data && (data = @unused_associations_data[name]) && data[assoc_name.to_s] == 'unused'
            return
          end
          
          super
        end

        # Setup the used_association_reflections storage before freezing
        def freeze
          used_association_reflections
          super
        end

        # Parse the coverage result, and return the coverage data for the
        # associations for descendants of this class. If the plugin
        # uses the :coverage_file option, the existing coverage file will be loaded
        # if present, and before the method returns, the coverage file will be updated.
        #
        # Options:
        # :coverage_result :: The coverage result to use. This defaults to +Coverage.result+.
        def update_associations_coverage(opts=OPTS)
          coverage_result = opts[:coverage_result] || Coverage.result
          module_mapping = {}
          file = @unused_associations_coverage_file

          coverage_data = if file && File.file?(file)
            Sequel.parse_json(File.binread(file))
          else
            {}
          end

          ([self] + descendants).each do |sc|
            next if sc.associations.empty? || !sc.name
            module_mapping[sc.send(:overridable_methods_module)] = sc
            cov_data = coverage_data[sc.name] ||= {''=>[]}
            cov_data[''].concat(sc.used_association_reflections.keys.map(&:to_s).sort).uniq!
          end

          coverage_result.each do |file, coverage|
            coverage[:methods].each do |(mod, meth), times|
              next unless sc = module_mapping[mod]
              coverage_data[sc.name][meth.to_s] ||= 0
              coverage_data[sc.name][meth.to_s] += times
            end
          end

          if file
            File.binwrite(file, Sequel.object_to_json(coverage_data))
          end

          coverage_data
        end

        # Parse the coverage data returned by #update_associations_coverage,
        # and return data on unused associations and unused association methods.
        #
        # Options:
        # :coverage_data :: The coverage data to use. If not given, it is taken
        #                   from the file specified by the :coverage_file plugin option.
        # :keep_coverage :: Do not delete the file specified by the :coverage_file plugin
        #                   option, even if it exists.
        def update_unused_associations_data(options=OPTS)
          coverage_data = options[:coverage_data] || Sequel.parse_json(File.binread(@unused_associations_coverage_file))

          unused_associations_data = {}
          to_many_modification_methods = [:adder, :remover, :clearer]
          modification_methods = [:setter, :adder, :remover, :clearer]

          ([self] + descendants).each do |sc|
            next unless cov_data = coverage_data[sc.name]
            reflection_data = cov_data[''] || []

            sc.association_reflections.each do |assoc, ref|
              # Only report associations for the class they are defined in
              next unless ref[:model] == sc

              # Do not report associations using methods_module option, because this plugin only
              # looks in the class's overridable_methods_module
              next if ref[:methods_module]

              info = {}
              if reflection_data.include?(assoc.to_s)
                info[:used] = [:reflection]
              end

              _update_association_coverage_info(info, cov_data, ref.dataset_method, :dataset_method)
              _update_association_coverage_info(info, cov_data, ref.association_method, :association_method)

              unless ref[:orig_opts][:read_only]
                if ref.returns_array?
                  _update_association_coverage_info(info, cov_data, ref[:add_method], :adder)
                  _update_association_coverage_info(info, cov_data, ref[:remove_method], :remover)
                  _update_association_coverage_info(info, cov_data, ref[:remove_all_method], :clearer)
                else
                  _update_association_coverage_info(info, cov_data, ref[:setter_method], :setter)
                end
              end

              next if info.keys == [:missing]

              if !info[:used]
                (unused_associations_data[sc.name] ||= {})[assoc.to_s] = 'unused'
              elsif unused = info[:unused]
                if unused.include?(:setter) || to_many_modification_methods.all?{|k| unused.include?(k)}
                  modification_methods.each do |k|
                    unused.delete(k)
                  end
                  unused << :read_only
                end
                (unused_associations_data[sc.name] ||= {})[assoc.to_s] = unused.map(&:to_s)
              end
            end
          end

          if @unused_associations_file
            File.binwrite(@unused_associations_file, Sequel.object_to_json(unused_associations_data))
          end
          unless options[:keep_coverage]
            _delete_unused_associations_file(@unused_associations_coverage_file)
          end

          unused_associations_data
        end

        # Return an array of unused associations.  These are associations where none of the
        # association methods are used, according to the coverage information.  Each entry
        # in the array is an array of two strings, with the first string being the class name
        # and the second string being the association name.
        #
        # Options:
        # :unused_associations_data :: The data to use for determining which associations
        #                              are unused, which is returned from
        #                              +update_unused_associations_data+. If not given,
        #                              loads the data from the file specified by the :file
        #                              plugin option.
        def unused_associations(opts=OPTS)
          unused_associations_data = opts[:unused_associations_data] || Sequel.parse_json(File.binread(@unused_associations_file))

          unused_associations = []
          unused_associations_data.each do |sc, associations|
            associations.each do |assoc, unused|
              if unused == 'unused'
                unused_associations << [sc, assoc]
              end
            end
          end
          unused_associations
        end

        # Return an array of unused association options.  These are associations some but not all
        # of the association methods are used, according to the coverage information. Each entry
        # in the array is an array of three elements.  The first element is the class name string,
        # the second element is the association name string, and the third element is a hash of
        # association options that can be used in the association so it does not define methods
        # that are not used.
        #
        # Options:
        # :unused_associations_data :: The data to use for determining which associations
        #                              are unused, which is returned from
        #                              +update_unused_associations_data+. If not given,
        #                              loads the data from the file specified by the :file
        #                              plugin option.
        def unused_association_options(opts=OPTS)
          unused_associations_data = opts[:unused_associations_data] || Sequel.parse_json(File.binread(@unused_associations_file))

          unused_association_methods = []
          unused_associations_data.each do |sc, associations|
            associations.each do |assoc, unused|
              unless unused == 'unused'
                unused_association_methods << [sc, assoc, set_unused_options_for_association({}, unused)]
              end
            end
          end
          unused_association_methods
        end

        # Delete the unused associations coverage file and unused associations data file,
        # if either exist.
        def delete_unused_associations_files
          _delete_unused_associations_file(@unused_associations_coverage_file)
          _delete_unused_associations_file(@unused_associations_file)
        end

        private

        # Delete the given file if it exists.
        def _delete_unused_associations_file(file)
          if file && File.file?(file)
            File.unlink(file)
          end
        end

        # Update the info hash with information on whether the given method was
        # called, according to the coverage information.
        def _update_association_coverage_info(info, coverage_data, meth, key)
          type = case coverage_data[meth.to_s]
          when 0
            :unused
          when Integer
            :used
          else
            # Missing here means there is no coverage information for the
            # the method, which indicates the expected method was never
            # defined.  In that case, it can be ignored.
            :missing
          end

          (info[type] ||= []) << key
        end

        # Based on the value of the unused, update the opts hash with association
        # options that will prevent unused association methods from being
        # defined.
        def set_unused_options_for_association(opts, unused)
          opts[:read_only] = true if unused.include?('read_only')
          opts[:no_dataset_method] = true if unused.include?('dataset_method')
          opts[:no_association_method] = true if unused.include?('association_method')
          opts[:adder] = nil if unused.include?('adder')
          opts[:remover] = nil if unused.include?('remover')
          opts[:clearer] = nil if unused.include?('clearer')
          opts
        end

        # If modifying associations, and this association has unused association
        # methods, automatically set the appropriate options so the unused association
        # methods are not defined, unless the association explicitly uses the :is_used
        # options.
        def def_association(opts)
          if !opts[:is_used] && @unused_associations_data && (data = @unused_associations_data[name]) && (unused = data[opts[:name].to_s])
            set_unused_options_for_association(opts, unused)
          end
          
          super
        end
      end
    end
  end
end
# :nocov:
