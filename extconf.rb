# ruby -r mkmf extconf.rb  --with-ingres-include="${II_SYSTEM}/ingres/files/" --with-ingres-lib="${II_SYSTEM}/ingres/lib/"

#
# if mkmf is not found on your system, install something like ruby1.8-dev
#
# May need to modify parameter to both "have_library" statements below
# depending on operating system and/or version of Ingres.  See NOTEs
# adjacent to each below.
#
require 'mkmf'
puts "Start extconf.rb"
dir_config('ingres')

if RUBY_PLATFORM !~ /mswin32/
    if ENV["II_SYSTEM"]
        puts "Using II_SYSTEM=#{ENV['II_SYSTEM']}"
        $CFLAGS += " -I#{ENV["II_SYSTEM"]}/ingres/files"
    end
end

if have_header('iiapi.h')
    if RUBY_PLATFORM =~ /mswin32/
        if have_library('oiapi','IIapi_initialize') # Check for Ingres 2.6
            puts "Found Ingres 2.6 or earlier.\n"
        elsif have_library('iilibapi','IIapi_initialize')
            puts "Found Ingres 2006 or later.\n"
        else
            puts "\n\nI couldn't find the Ingres libraries so I didn't create your Makefile.\n"
            puts "Do you have Ingres installed?\n"
            puts "Be sure to use the '--with-ingres-include' and '--with-ingres-lib' options.\n"
            puts "See the first line of extconf.rb for an example.\n\n"
            puts "BUILD FAILED\n\n"
            exit
        end
    else
        if ENV["II_SYSTEM"]
            libdir = ENV["II_SYSTEM"] + "/ingres/lib"
            $LDFLAGS += " -L#{libdir}"
            $LDFLAGS += " -lq.1 -lcompat.1 -lframe.1 -lrt -liiapi.1"
        end
       
        if RUBY_PLATFORM =~ /solaris/
            $LDFLAGS += " -lsocket"
        end

        if have_library('iiapi')
        else
            puts "\n\nI couldn't find the Ingres libraries so I didn't create your Makefile.\n"
            puts "Do you have Ingres installed?\n"
            puts "Be sure to use the '--with-ingres-include' and '--with-ingres-lib' options.\n"
            puts "See the first line of extconf.rb for an example.\n\n"
            puts "BUILD FAILED\n\n"
            exit
        end

        if ENV["INGRES_RUBY_DEBUG"] == "Y"
            $CFLAGS += " -g "
        end
    end 
    create_makefile('Ingres')
else
    puts "Unable to find iiapi.h, please verify your setup"
end
#do not remove the following line
#vim: ts=4 sw=4 expandtabs
