require 'Ingres'
require 'test/unit'
require 'tests/config.rb'

class TestIngresTransactionSavePoints < Test::Unit::TestCase

  # Data set used for inserting
  @@data = [[1, "one"], [2, "two"], [3, "three"], [4, "four"], [5, "five"], [6, "six"], [7, "seven"], [8, "eight"], [9, "nine"], [10, "ten"]]
  # Some savepoint => rollback values in the form of at savepoint x rollback to (=>) y
  @@rollback_points = { 5 => 3, 7 => 6 }

  def setup
    @@ing = Ingres.new()
    #@@ing.set_debug_flag("GLOBAL_DEBUG","TRUE")
    assert_kind_of(Ingres, @@ing.connect(@@database, @@username, @@password), "conn is not an Ingres object")
  end

  def teardown
    # drop the savepoint_tests table if it happens to be around still
    if @@ing.tables.include? "savepoint_tests"
      @@ing.execute "drop table savepoint_tests"
    end
    @@ing.disconnect
  end

  # A simple test to create a savepoint, if this fails then the rest of this unit test
  # has no chance
  def test_savepoint_single
    @@ing.execute "start transaction"
    @@ing.execute "create table savepoint_tests (id integer not null, txt varchar(100))"
    @@ing.savepoint "savepoint_1" 
    @@ing.execute "insert into savepoint_tests values (1, 'one')"
    @@ing.rollback
  end

  # Generate a savepoint for each insert into the table and rollback them all at the end
  def test_savepoint_multiple
    @@ing.execute "start transaction"
    @@ing.execute "create table savepoint_tests (id integer not null, txt varchar(100))"
    @@data.each { |element|
        @@ing.execute("insert into savepoint_tests values (#{element[0]}, '#{element[1]}')")
        @@ing.savepoint "savepoint_#{element[0]}"
    }
    @@ing.rollback
  end

  # Generate a savepoint for each insert then rollback to specified savepoints verifying 
  # we have actually rolled back
  def test_savepoint_named_rollback
    @@ing.execute "start transaction"
    @@ing.execute "create table savepoint_tests (id integer not null, txt varchar(100))"
    @@data.each { |element|
        @@ing.execute("insert into savepoint_tests values (#{element[0]}, '#{element[1]}')")
        @@ing.savepoint "savepoint_#{element[0]}"
    }
    @@ing.rollback "savepoint_8"
    assert_equal 8, @@ing.execute("select max(id) from savepoint_tests").flatten[0]
    @@ing.rollback "savepoint_5"
    assert_equal 5, @@ing.execute("select max(id) from savepoint_tests").flatten[0]
    @@ing.rollback "savepoint_3"
    assert_equal 3, @@ing.execute("select max(id) from savepoint_tests").flatten[0]
    @@ing.rollback
  end

  # Test the ability to carry on working after a rollback has been issued against a named savepoint.
  # Then carry on inserting then rollback again.
  def test_savepoint_nested_transactions
    @@ing.execute "start transaction"
    @@ing.execute "create table savepoint_tests (id integer not null, txt varchar(100))"
    @@data.each { |data_set|
      @@ing.execute("insert into savepoint_tests values (#{data_set[0]}, '#{data_set[1]}')")
      @@ing.savepoint "savepoint_#{data_set[0]}"
      if @@rollback_points.has_key?(data_set[0])
        @savepoint_no = @@rollback_points.values_at(data_set[0])
        @@ing.rollback "savepoint_#{@savepoint_no}"
        assert_equal @savepoint_no, @@ing.execute("select max(id) from savepoint_tests").flatten
      end
    }
    @@ing.rollback
  end

  # Verify the driver raises a RuntimeError exception when rolling back to a non-existant
  # savepoint
  def test_savepoint_rollback_non_existent
    @@ing.execute "start transaction"
    @@ing.execute "create table savepoint_tests (id integer not null, txt varchar(100))"
    @@data.each { |data_set|
      @@ing.execute("insert into savepoint_tests values (#{data_set[0]}, '#{data_set[1]}')")
      @@ing.savepoint "savepoint_#{data_set[0]}"
      if (data_set[0]) == 5
        @@ing.rollback "savepoint_3"
        assert_equal 3, @@ing.execute("select max(id) from savepoint_tests").flatten[0]
      end
    }
    assert_raise RuntimeError do
      @@ing.rollback "savepoint_5"
    end
  end

  # Ingres requires that a transaction has been started using "start transaction" before
  # a savepoint can be created
  def test_savepoint_no_transaction
    # Cannot start a savepoint with no active transaction
    assert_raise RuntimeError do
      @@ing.savepoint "savepoint_1" 
    end
  end

end
