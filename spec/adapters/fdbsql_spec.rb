SEQUEL_ADAPTER_TEST = :fdbsql

require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe 'Fdbsql' do
  describe 'Database' do
    before(:all) do
      @db = DB
    end

    describe 'schema_parsing' do
      after do
        @db.drop_table?(:test)
      end

      it 'without primary key' do
        @db.create_table(:test) do
          text :name
          int :value
        end
        schema = DB.schema(:test, :reload => true)
        schema.count.must_equal 2
        schema[0][0].must_equal :name
        schema[1][0].must_equal :value
        schema.each {|col| col[1][:primary_key].must_equal nil}
      end

      it 'with one primary key' do
        @db.create_table(:test) do
          text :name
          primary_key :id
        end
        schema = DB.schema(:test, :reload => true)
        schema.count.must_equal 2
        id_col = schema[0]
        name_col = schema[1]
        name_col[0].must_equal :name
        id_col[0].must_equal :id
        name_col[1][:primary_key].must_equal nil
        id_col[1][:primary_key].must_equal true
      end

      it 'with multiple primary keys' do
        @db.create_table(:test) do
          Integer :id
          Integer :id2
          primary_key [:id, :id2]
        end
        schema = DB.schema(:test, :reload => true)
        schema.count.must_equal 2
        id_col = schema[0]
        id2_col = schema[1]
        id_col[0].must_equal :id
        id2_col[0].must_equal :id2
        id_col[1][:primary_key].must_equal true
        id2_col[1][:primary_key].must_equal true
      end

      it 'with other constraints' do
        @db.create_table(:test) do
          primary_key :id
          Integer :unique, :unique => true
        end
        schema = DB.schema(:test, :reload => true)
        schema.count.must_equal 2
        id_col = schema[0]
        unique_col = schema[1]
        id_col[0].must_equal :id
        unique_col[0].must_equal :unique
        id_col[1][:primary_key].must_equal true
        unique_col[1][:primary_key].must_equal nil
      end
      after do
        @db.drop_table?(:other_table)
      end
      it 'with other tables' do
        @db.create_table(:test) do
          Integer :id
          text :name
        end
        @db.create_table(:other_table) do
          primary_key :id
          varchar :name, :unique => true
        end
        schema = DB.schema(:test, :reload => true)
        schema.count.must_equal 2
        schema.each {|col| col[1][:primary_key].must_equal nil}
      end

      describe 'with explicit schema' do
        before do
          @db.create_table(:test) do
            primary_key :id
          end
          @schema = @db['SELECT CURRENT_SCHEMA'].first.values.first
          @second_schema = @schema + "--2"
          @db.create_table(Sequel.qualify(@second_schema,:test)) do
            primary_key :id2
            Integer :id
          end
        end
        after do
          @db.drop_table?(Sequel.qualify(@second_schema,:test))
          @db.drop_table?(:test)
        end

        it 'gets info for correct table' do
          schema = DB.schema(:test, :reload => true, :schema => @second_schema)
          schema.count.must_equal 2
          id2_col = schema[0]
          id_col = schema[1]
          id_col[0].must_equal :id
          id2_col[0].must_equal :id2
          id_col[1][:primary_key].must_equal nil
          id2_col[1][:primary_key].must_equal true
        end
      end
    end

    describe 'primary_key' do
      after do
        @db.drop_table?(:test)
        @db.drop_table?(:other_table)
      end

      it 'without primary key' do
        @db.create_table(:test) do
          text :name
          int :value
        end
        DB.primary_key(:test).must_equal nil
      end

      it 'with one primary key' do
        @db.create_table(:test) do
          text :name
          primary_key :id
        end
        DB.primary_key(:test).must_equal :id
      end

      it 'with multiple primary keys' do
        @db.create_table(:test) do
          Integer :id
          Integer :id2
          primary_key [:id, :id2]
        end
        DB.primary_key(:test).must_equal [:id, :id2]
      end

      it 'with other constraints' do
        @db.create_table(:test) do
          primary_key :id
          Integer :unique, :unique => true
        end
        DB.primary_key(:test).must_equal :id
      end

      it 'with other tables' do
        @db.create_table(:test) do
          Integer :id
          text :name
        end
        @db.create_table(:other_table) do
          primary_key :id
          varchar :name, :unique => true
        end
        DB.primary_key(:other_table).must_equal :id
      end

      it 'responds to alter table' do
        @db.create_table(:test) do
          Integer :id
          text :name
        end
        @db.alter_table(:test) do
          add_primary_key :quid
        end
        DB.primary_key(:test).must_equal :quid
      end

      describe 'with explicit schema' do
        before do
          @db.create_table(:test) do
            primary_key :id
          end
          @schema = @db['SELECT CURRENT_SCHEMA'].first.values.first
          @second_schema = @schema + "--2"
          @db.create_table(Sequel.qualify(@second_schema,:test)) do
            primary_key :id2
          end
        end
        after do
          @db.drop_table?(Sequel.qualify(@second_schema,:test))
          @db.drop_table?(:test)
        end

        it 'gets correct primary key' do
          DB.primary_key(:test, :schema => @second_schema).must_equal :id2
        end
      end
    end

    describe '#tables' do
      before do
        @schema = @db['SELECT CURRENT_SCHEMA'].first.values.first
        @second_schema = @schema + "--2"
        @db.create_table(:test) do
          primary_key :id
        end
        @db.create_table(Sequel.qualify(@second_schema,:test2)) do
          primary_key :id
        end
      end
      after do
        @db.drop_table?(Sequel.qualify(@second_schema,:test2))
        @db.drop_table?(:test)
      end
      it 'on explicit schema' do
        tables = @db.tables(:schema => @second_schema)
        tables.must_include(:test2)
        tables.wont_include(:test)
      end
      it 'qualified' do
        tables = @db.tables(:qualify => true)
        tables.must_include(Sequel::SQL::QualifiedIdentifier.new(@schema.to_sym, :test))
        tables.wont_include(:test)
      end
    end

    describe '#views' do
      def drop_things
        @db.drop_view(Sequel.qualify(@second_schema,:test_view2), :if_exists => true)
        @db.drop_table?(Sequel.qualify(@second_schema,:test_table))
        @db.drop_view(:test_view, :if_exists => true)
        @db.drop_table?(:test_table)
      end
      before do
        @schema = @db['SELECT CURRENT_SCHEMA'].single_value
        @second_schema = @schema + "--2"
        drop_things
        @db.create_table(:test_table){Integer :a}
        @db.create_view :test_view, @db[:test_table]
        @db.create_table(Sequel.qualify(@second_schema,:test_table)) do
          Integer :b
        end
        @db.create_view(Sequel.qualify(@second_schema, :test_view2),
                        @db[Sequel.qualify(@second_schema, :test_table)])
      end
      after do
        drop_things
      end
      it 'on explicit schema' do
        views = @db.views(:schema => @second_schema)
        views.must_include(:test_view2)
        views.wont_include(:test_view)
      end
      it 'qualified' do
        views = @db.views(:qualify => true)
        views.must_include(Sequel::SQL::QualifiedIdentifier.new(@schema.to_sym, :test_view))
        views.wont_include(:test)
      end
    end

    describe 'prepared statements' do
      def create_table
        @db.create_table!(:test) {Integer :a; Text :b}
        @db[:test].insert(1, 'blueberries')
        @db[:test].insert(2, 'trucks')
        @db[:test].insert(3, 'foxes')
      end
      def drop_table
        @db.drop_table?(:test)
      end
      before do
        create_table
      end
      after do
        drop_table
      end

      it 're-prepares on stale statement' do
        @db[:test].filter(:a=>:$n).prepare(:all, :select_a).call(:n=>2).to_a.must_equal [{:a => 2, :b => 'trucks'}]
        drop_table
        create_table
        @db[:test].filter(:a=>:$n).prepare(:all, :select_a).call(:n=>2).to_a.must_equal [{:a => 2, :b => 'trucks'}]
      end

      it 'can call already prepared' do
        @db[:test].filter(:a=>:$n).prepare(:all, :select_a).call(:n=>2).to_a.must_equal [{:a => 2, :b => 'trucks'}]
        drop_table
        create_table
        @db.call(:select_a, :n=>2).to_a.must_equal [{:a => 2, :b => 'trucks'}]
      end
    end

    describe 'Database schema modifiers' do
      # this test was copied from sequel's integration/schema_test because that one drops a serial primary key which is not
      # currently supported in fdbsql
      it "should be able to specify constraint names for column constraints" do
        @db.create_table!(:items2){Integer :id, :primary_key=>true, :primary_key_constraint_name=>:foo_pk}
        @db.create_table!(:items){foreign_key :id, :items2, :unique=>true, :foreign_key_constraint_name => :foo_fk, :unique_constraint_name => :foo_uk, :null=>false}
        @db.alter_table(:items){drop_constraint :foo_fk, :type=>:foreign_key; drop_constraint :foo_uk, :type=>:unique}
        @db.alter_table(:items2){drop_constraint :foo_pk, :type=>:primary_key}
      end
    end
  end

  describe 'Dataset' do
    before(:all) do
      @db = DB
    end

    describe 'provides_accurate_rows_matched' do
      before do
        DB.create_table!(:test) {Integer :a}
        DB[:test].insert(1)
        DB[:test].insert(2)
        DB[:test].insert(3)
        DB[:test].insert(4)
        DB[:test].insert(5)
      end

      after do
        DB.drop_table?(:test)
      end

      it '#delete' do
        DB[:test].where(:a => 8..10).delete.must_equal 0
        DB[:test].where(:a => 5).delete.must_equal 1
        DB[:test].where(:a => 1..3).delete.must_equal 3
      end

      it '#update' do
        DB[:test].where(:a => 8..10).update(:a => Sequel.+(:a, 10)).must_equal 0
        DB[:test].where(:a => 5).update(:a => Sequel.+(:a, 1000)).must_equal 1
        DB[:test].where(:a => 1..3).update(:a => Sequel.+(:a, 100)).must_equal 3
      end

    end

    describe 'intersect and except ALL' do
      before do
        DB.create_table!(:test) {Integer :a; Integer :b}
        DB[:test].insert(1, 10)
        DB[:test].insert(2, 10)
        DB[:test].insert(8, 15)
        DB[:test].insert(2, 10)
        DB[:test].insert(2, 10)
        DB[:test].insert(1, 10)

        DB.create_table!(:test2) {Integer :a; Integer :b}
        DB[:test2].insert(1, 10)
        DB[:test2].insert(2, 10)
        DB[:test2].insert(2, 12)
        DB[:test2].insert(3, 10)
        DB[:test2].insert(1, 10)
      end

      after do
        DB.drop_table?(:test)
        DB.drop_table?(:test2)
      end

      it 'intersect all' do
        @db[:test].intersect(@db[:test2], :all => true).map{|r| [r[:a],r[:b]]}.to_a.sort.must_equal [[1, 10], [1,10], [2, 10]]
      end

      it 'except all' do
        @db[:test].except(@db[:test2], :all => true).map{|r| [r[:a],r[:b]]}.to_a.sort.must_equal [[2,10], [2, 10], [8,15]]
      end
    end

    describe 'is' do
      before do
        DB.create_table!(:test) {Integer :a; Boolean :b}
        DB[:test].insert(1, nil)
        DB[:test].insert(2, true)
        DB[:test].insert(3, false)
      end
      after do
        DB.drop_table?(:test)
      end

      it 'true' do
        DB[:test].select(:a).where(Sequel::SQL::ComplexExpression.new(:IS, :b, true)).map{|r| r[:a]}.must_equal [2]
      end

      it 'not true' do
        DB[:test].select(:a).where(Sequel::SQL::ComplexExpression.new(:'IS NOT', :b, true)).map{|r| r[:a]}.must_equal [1, 3]
      end
    end

    describe 'insert empty values' do
      before do
        DB.create_table!(:test) {primary_key :a}
      end
      after do
        DB.drop_table?(:test)
      end

      it 'inserts defaults and returns pk' do
        DB[:test].insert().must_equal 1 # 1 should be the pk
      end
    end

    describe 'function names' do
      before do
        DB.create_table!(:test) {Text :a; Text :b}
        DB[:test].insert('1', '')
        DB[:test].insert('2', 'trucks')
        DB[:test].insert('3', 'foxes')
      end
      after do
        DB.drop_table?(:test)
      end

      it 'evaluate' do
        DB[:test].select(Sequel.function(:now)).count == 1
        DB[:test].select(Sequel.as(Sequel.function(:concat, :a, :b), :c)).map{|r| r[:c]}.must_equal ['1','2trucks','3foxes']
      end

      it 'get quoted' do
        DB[:test].select(Sequel.function(:now).quoted).sql.must_match /"now"\(\)/
        DB[:test].select(Sequel.as(Sequel.function(:concat, :a, :b).quoted, :c)).sql.must_match /"concat"\("a", "b"\)/
      end
    end
  end
end
