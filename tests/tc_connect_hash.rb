require 'Ingres'
require 'test/unit'
require 'tests/config.rb'

class TestIngresConnectionHash < Test::Unit::TestCase

  def test_connect_hash
    ing = Ingres.new()
    assert_kind_of(Ingres, ing.connect(:database => @@database), "conn is not an Ingres object")
  end

  def test_connect_hash_no_database
    ing = Ingres.new()
    assert_raise ArgumentError do
        ing.connect()
    end
  end

  def test_connect_hash_with_login_password
    ing = Ingres.new()
    assert_kind_of(Ingres, ing.connect_with_credentials(:database => @@database, :username => @@username, :password => @@password), "conn is not an Ingres object")
  end
 
  def test_connect_hash_with_login_only
    ing = Ingres.new()
    assert_raise ArgumentError do
        ing.connect_with_credentials(:database => @@database, :username => @@username)
    end
  end

end
