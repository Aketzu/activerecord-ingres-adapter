require 'Ingres'
require 'test/unit'
require 'ext/tests/config.rb'

class TestIngresConnection < Test::Unit::TestCase

  def test_connect_with_login_only
    ing = Ingres.new()
    assert_raise ArgumentError do
        ing.connect_with_credentials(@@database,@@username)
    end
  end

end
