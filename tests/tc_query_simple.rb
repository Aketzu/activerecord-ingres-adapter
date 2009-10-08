require 'Ingres'
require 'test/unit'
require 'tests/config.rb'

class TestIngresQuerySimple < Test::Unit::TestCase

  def setup
    @@ing = Ingres.new()
    assert_kind_of(Ingres, @@ing.connect(@@database, @@username, @@password), "conn is not an Ingres object")
  end

  def teardown
    @@ing.disconnect
  end

  def test_query_username
    # ing.execute returns and array so we want the first and only element
    assert_equal @@username, @@ing.execute("select dbmsinfo('session_user')")[0]
  end

  def test_tables
    if @@database == "demodb"
      assert_not_nil @@ing.tables.include? "airport"
      assert_not_nil @@ing.tables.include? "country"
      assert_not_nil @@ing.tables.include? "user_profile"
    end
  end
 
end
