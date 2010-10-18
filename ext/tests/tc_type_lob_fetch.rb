require 'Ingres'
require 'test/unit'
require 'ext/tests/config.rb'

class TestIngresTypeLOB < Test::Unit::TestCase
  def setup
    @@ing = Ingres.new()
    assert_kind_of(Ingres, @@ing.connect(@@database), "conn is not an Ingres object")
  end

  def teardown
    @@ing.disconnect
  end

  def test_blob_fetch
      sql = "select up_image from user_profile where up_id = 1"
      data = @@ing.execute(sql)
  end
 
end
