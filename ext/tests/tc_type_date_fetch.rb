require 'Ingres'
require 'test/unit'
require 'ext/tests/config.rb'

class TestIngresTypeDateFetch< Test::Unit::TestCase
  def setup
    @@ing = Ingres.new()
    assert_kind_of(Ingres, @@ing.connect(@@database), "conn is not an Ingres object")
  end

  def teardown
    @@ing.disconnect
  end

  def test_ingresdate_fetch
      sql = "select rt_depart_at, rt_arrive_at from route where rt_depart_from = 'VLL' and rt_arrive_to='MAD' and rt_id=1305"
      data = @@ing.execute(sql).flatten
      assert_equal "11-oct-2006 19:00:00", data[0]
      assert_equal "12-oct-2006 05:27:00", data[1]
  end
 
end
