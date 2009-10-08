# Copyright (c) 2009 Ingres Corporation
#
# Ruby unit tests for the Ingres Ruby driver using the Ruby Test::Unit 
# Framework see, 
# (http://www.ruby-doc.org/stdlib/libdoc/test/unit/rdoc/classes/Test/Unit.html)
# for more information
#
# Add all new test suites and test cases (if they are not part of a test suite)
# to this file
#
# To execute the tests:
#    ruby tests/ts_all_rb
# To execute an individual suite:
#    ruby tests/ts_connect.rb
# Or to execute an individual test case:
#    ruby tests/tc_connect.rb
#
# Naming conventions for test files
#   test suites to be prefixed with ts_
#   test cases to be prefixed with tc_

# Test case for local connection
#
require 'tests/ts_connect.rb'
require 'tests/ts_execute.rb'
require 'tests/ts_types.rb'
require 'tests/ts_transactions.rb'
