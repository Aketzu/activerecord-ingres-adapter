# Ingres Ruby ActiveRecord Adapter/Driver

This repository contains code based on the outdated gem provided by Actian support
for recent Ingres releases and Rails 3/4 compatibility. Bear in mind that not everything
may be functional. As of today here's a list of what you can do (will update this as I go):

* `SELECT` statements are fully functionals.

## Overview

Three components provide Ingres support for Ruby:

* Ingres Ruby driver - native SQL-level interface
* Ingres ActiveRecord adapter - object relational adapater for Ruby on Rails
* Ingres Arel Visitor binding - specific visitor bindings for Arel

All components are required for Rails applications using ActiveRecord,
which is the standard Rails Object-to-Relations Mapping (ORM) interface. The
adapter implements the ActiveRecord classes for Ingres; internally, it invokes the
classes and methods in the driver to actually communicate with Ingres.
The driver, while primarily intended to service the adapter, can also be
invoked directly from any Ruby program to communicate with Ingres. The driver
interface does not follow any standard API and may change in the future.

## Installation Considerations

To build and install the Ingres Ruby interface, the following components are
required:

* Ingres 9.1 or above. At a minimum, an Ingres client installation is required
  on the same machine as the Ruby installation. For a list of Ingres binary and
  source downloads, see http://esd.ingres.com.
* Ruby, preferably at version 1.9.3 or later. Additional Ruby components or
  related products (such as ActiveRecord or Rails) may also be required
  depending on how Ruby will be used with Ingres. See below for details.
* C compiler (for example, GNU/C on Linux and UNIX, or Microsoft Visual Studio
  6 on Windows). The C compiler is not needed if you are using using a pre-built 
  version of the Ingres Ruby driver.
  Note: It is not possible to build Ruby extensions with newer versions of the
  Visual Studio C compiler without making changes to the Ruby header files.
* The Ingres Ruby ActiveRecord adapter source code and driver binary. The source 
  code for the driver also is needed if not using the pre-built binary.

## Installation

### Rails 3.2.x

I maintain compatibility for Rails 3.2.x and Arel 3 in the 3-0-stable branch. Using rubygems you can do:

    $ gem install activerecord-ingres-adapter -v 3.0.0

### Rails 4

I maintain compatibility for Rails 4 and Arel 4/5 in the 4-0-stable branch. Using rubygems you can do:

    $ gem install activerecord-ingres-adapter -v 4.0.0

## TODO

* Add support for `INSERT`, `UPDATE`, `DELETE` statements.
* Add support for table manipulation/migrations.
* Add support for database creation/deletion.

## Notes

I'm not in any part working for Actian, I'm just a third-party developer trying to make do
with what I can. If some things aren't working I'm willing to help and provide limited support.

If you want to help you're more than welcome to.
