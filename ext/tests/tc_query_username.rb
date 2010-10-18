require 'Ingres'
require 'test/unit'
require 'ext/tests/config.rb'

class TestIngresQueryUsername < Test::Unit::TestCase

  def setup
    @ing = Ingres.new()
    @ing.connect(@@database, @@username, @@password)
  end

  def test_query_username
    # ing.execute returns and array so we want the first and only element
    assert_equal @@username, @ing.execute("select dbmsinfo('username')")[0][0]
  end

  def test_tables
    # ing.execute returns and array so we want the first and only element
    assert_not_nil @ing.tables.include? "airport"
  end
 
end
