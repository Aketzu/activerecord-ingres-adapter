require 'Ingres'
require 'test/unit'
require 'ext/tests/config.rb'

class TestIngresConnection < Test::Unit::TestCase

  def test_connect_with_login_password
    ing = Ingres.new()
    assert_kind_of(Ingres, ing.connect_with_credentials(@@database,@@username,@@password), "conn is not an Ingres object")
  end

end
