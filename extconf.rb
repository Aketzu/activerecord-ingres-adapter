# UNIX/Linux/OS X
# ruby -r mkmf extconf.rb  --with-ingres-include="${II_SYSTEM}/ingres/files/" --with-ingres-lib="${II_SYSTEM}/ingres/lib/"
# Windows
# ruby -r mkmf extconf.rb  --with-ingres-include="%II_SYSTEM%\ingres\files\" --with-ingres-lib="%II_SYSTEM%\ingres\lib\"
#
# If mkmf is not found on your system, install something like ruby1.8-dev
#
require 'mkmf'
puts "Start extconf.rb"

#Get the supplied library/header directory (if any)
ingres_dirs=dir_config('ingres')

#Sanity checks, --with-ingres-xxx override system set II_SYSTEM
if ingres_dirs[0] =~ /files/ and ingres_dirs[1] =~ /lib/ 
    puts "Using supplied directories:"
    puts "Headers   - #{ingres_dirs[0]}"
    puts "Libraries - #{ingres_dirs[1]}"

elsif ENV["II_SYSTEM"] # fallback
    puts "Using II_SYSTEM=#{ENV['II_SYSTEM']}"
    $CFLAGS += " -I#{ENV["II_SYSTEM"]}/ingres/files"
    if RUBY_PLATFORM !~ /mswin32/
	libdir = ENV["II_SYSTEM"] + "/ingres/lib"
        $LDFLAGS += " -L#{libdir}"
        $LDFLAGS += " -lq.1 -lcompat.1 -lframe.1 -lrt -liiapi.1"
        if RUBY_PLATFORM =~ /solaris/
            $LDFLAGS += " -lsocket"
        end
    else
	libdir = "#{ENV["II_SYSTEM"]}\\ingres\\lib"
        $LIBPATH.push("-libpath:\"#{libdir}\"")
    end
else
    puts "Unable to proceed without the environment variable II_SYSTEM or the flags --with-ingres-files/--with-ingres-lib pointing to a valid Ingres installation"
    exit(1)
end

if have_header('iiapi.h')
    if RUBY_PLATFORM =~ /mswin32/
        if have_library('oiapi','IIapi_initialize') # Check for Ingres 2.6
            have_library('ingres')
            puts "Found Ingres 2.6 or earlier.\n"
        elsif have_library('iilibapi','IIapi_initialize')
            have_library('libingres')
            puts "Found Ingres 2006/9.0.4 or later.\n"
        else
            puts "\n\nI couldn't find the Ingres libraries so I didn't create your Makefile.\n"
            puts "Do you have Ingres installed?\n"
            puts "Be sure to use the '--with-ingres-include' and '--with-ingres-lib' options.\n"
            puts "See the first line of extconf.rb for an example.\n\n"
            puts "BUILD FAILED\n\n"
            exit
        end
    else

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
    $OBJS=['Unicode.o','Ingres.c']

    create_makefile('Ingres')
else
    puts "Unable to find iiapi.h, please verify your setup"
end
#do not remove the following line
#vim: ts=4 sw=4 expandtabs
