require 'Ingres'
require 'test/unit'
require 'tests/config.rb'

class TestIngresEnvionmentDateFormat < Test::Unit::TestCase

  def test_connect_env_date_iso4
    @@ing = Ingres.new()
    @@ing.connect(:database => @@database, :date_format => Ingres::DATE_FORMAT_ISO4)
    assert_equal "20091109 15:14:00", @@ing.execute("select date('20091109 15:14:00')").flatten[0]
  end

  def test_connect_env_date_finland
    @@ing = Ingres.new()
    @@ing.connect(:database => @@database, :date_format => Ingres::DATE_FORMAT_FINLAND)
    assert_equal "2009-11-09 15:14:00", @@ing.execute("select date('2009-11-09 15:14:00')").flatten[0]
  end

  def test_connect_env_date_multinational4
    @@ing = Ingres.new()
    @@ing.connect(:database => @@database, :date_format => Ingres::DATE_FORMAT_MULTINATIONAL4)
    assert_equal "09/11/2009 15:14:00", @@ing.execute("select date('09/11/2009 15:14:00')").flatten[0]
  end

  # Same as above but using Ingres.set_environment()
  def test_set_env_date_iso4
    @@ing = Ingres.new()
    @@ing.connect(:database => @@database)
    @@ing.set_environment(:date_format => Ingres::DATE_FORMAT_ISO4)
    assert_equal "20061012 01:09:00", @@ing.execute("select rt_arrive_at, rt_depart_at from route where rt_depart_from = 'VLL' and rt_arrive_to = 'LHR'")[0][0]
  end

  def test_set_env_date_finland
    @@ing = Ingres.new()
    @@ing.connect(:database => @@database)
    @@ing.set_environment(:date_format => Ingres::DATE_FORMAT_FINLAND)
    assert_equal "2006-10-11 20:26:00", @@ing.execute("select rt_arrive_at, rt_depart_at from route where rt_depart_from = 'VLL' and rt_arrive_to = 'LHR'")[0][1]
  end

  def test_set_env_date_multinational4
    @@ing = Ingres.new()
    @@ing.connect(:database => @@database)
    @@ing.set_environment(:date_format => Ingres::DATE_FORMAT_MULTINATIONAL4)
    assert_equal "11/10/2006 23:40:00", @@ing.execute("select rt_arrive_at, rt_depart_at from route where rt_depart_from = 'VLL' and rt_arrive_to = 'LHR'")[1][0]
  end

end
