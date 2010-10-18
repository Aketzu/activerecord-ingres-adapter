# Database and connection information for the tests
# Use a vnode for Windows as the build box only has a
# client installation
if RUBY_PLATFORM =~ /mswin32/
    @@database = "ruby::demodb"
else
    @@database = "demodb"
end
@@username = "ruby"
@@password = "ruby"
 
