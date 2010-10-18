require 'Ingres'
require 'test/unit'
require 'ext/tests/config.rb'

class TestIngresDualConnections < Test::Unit::TestCase

  def test_dual_connections
    ing = Ingres.new()
    assert_kind_of(Ingres, ing.connect(@@database), "conn is not an Ingres object")
    ing2 = Ingres.new()
    assert_kind_of(Ingres, ing2.connect(@@database), "conn is not an Ingres object")
  end

end
