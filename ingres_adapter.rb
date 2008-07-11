#--
# Copyright (c) 2007 Ingres Corporation
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as 
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
# ingres_adapter.rb
#
# History
#       12/18/06 (Jared Richardson)
#           Community developer provided original source 
#           for Ingres Corp to use as basis for Ruby driver
#       12/19/07 (grant.croker@ingres.com)
#           Fix quote() to use the class variable rather than 
#           the equivalent Ingres column type.   
#       01/17/08 (grant.croker@ingres.com)
#           Add GPL License header
#       06/27/08 (grant.croker@ingres.com)
#           Add support for Rails Migrations.
#       06/27/08 (bruce.lunsford@ingres.com)
#           Add support for Ingres datatypes float4, float8, and money
#           as AR :float and Ruby Float; previously, Ingres floats
#           were mapped to AR nil and Ruby String. Remove unneeded/unused
#           code such as SMALLINT and xxx_simplified_type() methods.
#           Add mapping for Ingres ANSI date/time formats and change
#           INGRESDATE (=DATE in pre-ANSI Ingres) mapping from :date to
#           :datetime.
#       07/02/08 (grant.croker@ingres.com)
#           Add quote_column_names() for quoting of table and column names. 
#           Required for the ActiveRecord base unit tests
#       07/02/08 (grant.croker@ingres.com)
#           Fix data truncation for the ActiveRecord :text type
#++



require 'active_record/connection_adapters/abstract_adapter'
require 'pp'

#debug print routines

PRINT_TRACES = false

def debug_level
   return 0
end

def debug_print (msg, context=nil)
   puts "\nDebug Print::\n"
   puts msg
   if context then
      puts context
   end
end

def complete_trace (msg)
   if(PRINT_TRACES) then
      puts "\n#{msg}\n"
   end
end



module ActiveRecord
   class Base
      # Establishes a connection to the database that's used by all Active Record objects
      def self.ingres_connection(config) # :nodoc:
         require_library_or_gem 'Ingres' unless self.class.const_defined?(:Ingres)

         complete_trace("in ingres_connection(config) ")

         ing = Ingres.new

         config = config.symbolize_keys

         if config.has_key?(:database)
            database = config[:database]
         else
            raise ArgumentError, "No database specified. Missing argument: database."
         end

         if config.has_key?(:username)
            username = config[:username].to_s
            password = config[:password].to_s
            conn = ing.connect_with_credentials(database, username, password)
         else
            # ingres allows a local connection w/o username and password
            conn = ing.connect(database)
         end

         #return the connection adapter
         ConnectionAdapters::IngresAdapter.new(conn, logger)
      end
   end

   module ConnectionAdapters

      class IngresColumn < Column #:nodoc:

         def value_to_boolean (str)

            if(str.to_i==0)
               return 0
            else
               return 1
            end
         end

         def initialize(name, default, sql_type_hash = nil, null = true)

            complete_trace(" in initialize (#{name}, #{default}, #{sql_type_hash.inspect}, #{null})")

            super(name, default, sql_type_hash["column_type"], null)

         end

         def type_cast(value)
            complete_trace(" in type_cast(#{value}) w/@type=#{@type}")

            return nil if value.nil?
            case @type
            when :string    then value
            when :text      then value
            when :float     then value.to_f
            when :binary    then self.class.binary_to_string(value)
            when :boolean   then self.class.value_to_boolean(value)
            when :integer   then value.to_i rescue value ? 1 : 0
            when :datetime  then self.class.string_to_time(value)
            when :timestamp then self.class.string_to_time(value)
            when :time      then self.class.string_to_dummy_time(value)
            when :date
               res = self.class.string_to_date(value)
               if(@name =~ /_time$/ ) then
                  value=value[11,19]
                  value = "2000-01-01 " << value
                  #res = self.class.string_to_dummy_time(value)
                  res = Time.parse(value)
               else
                  res = self.class.string_to_date(value)
               end
               res
            else
               value
            end
         end

         def type_cast_code(var_name)
            complete_trace(" in type_cast_code(#{var_name}) ")
            case type
            when :string    then nil
            when :text      then nil
            when :integer   then "(#{var_name}.to_i rescue #{var_name} ? 1 : 0)"
            when :float     then "#{var_name}.to_f"
            when :datetime  then "#{self.class.name}.string_to_time(#{var_name})"
            when :timestamp then "#{self.class.name}.string_to_time(#{var_name})"
            when :time      then "#{self.class.name}.string_to_dummy_time(#{var_name})"
            when :binary    then "#{self.class.name}.binary_to_string(#{var_name})"
            when :boolean   then "#{self.class.name}.value_to_boolean(#{var_name})"

            when :date
               #str = "#{self.class.name}.string_to_date(#{var_name})"
               str = "if(@name =~ /_time$/ ) then \n"
               str << " #{var_name}=#{var_name}[11,19] \n"
               str << " value = '2000-01-01' << #{var_name} \n"
               str << " res = #{self.class.name}.string_to_time(#{var_name}) \n"
               str << " else \n"
               str << " res = #{self.class.name}.string_to_date(#{var_name}) \n"
               str << " end\n"
               # puts "\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
               # puts ">>#{str}<<"

               str
            else nil
            end
         end


         private
         def simplified_type(field_type)
            complete_trace(" in simplified_type(#{field_type}) ")
            case field_type
            when /INTEGER/
               :integer
            when /FLOAT|MONEY/
               :float
            when /BOOLEAN/
               :boolean
            when /VARCHAR/
               :string
            when /CHAR/
               :string
            when /ANSIDATE/
               :date
            when /DATE|INGRESDATE/
               :datetime
            when /TIMESTAMP/
               :timestamp
            when /TIME/
               :time
            when /INTERVAL/    # No equivalent in Rails
               :string
            end
         end
      end


      # This Ingres adapter works with the Ruby/Ingres driver written
      # by Jared Richardson
      #
      # Options:
      #
      # * <tt>:username</tt> - Optional-Defaults to nothing
      # * <tt>:password</tt> - Optional-Defaults to nothing
      # * <tt>:database</tt> - The name of the database. No default, must be provided.
      #
      # Maintainer: jared@jaredrichardson.net
      #
      class IngresAdapter < AbstractAdapter
         def adapter_name
            'Ingres'
         end

         def native_database_types
            complete_trace(" in native_database_type ")
            {
               :primary_key => "integer NOT NULL PRIMARY KEY",
               :string      => { :name => "varchar", :limit => 255 },
               :text        => { :name => "varchar", :limit => 32000 },
               :integer     => { :name => "integer" },
               :float       => { :name => "float" },
               :datetime    => { :name => "ingresdate" },
               :timestamp   => { :name => "timestamp" },
               :time        => { :name => "time" },
               :date        => { :name => "ansidate" }, #"date" pre-ansidate
               :binary      => { :name => "blob" },
               :boolean     => { :name => "tinyint" }
            }
         end

         def get_data_size(id)
            complete_trace(" in get_data_size(#{id}) ")
            column_names = @connection.column_list_of_names
            index = column_names.index(id)

            if(index) then
               data_sizes = @connection.data_sizes
               res = data_sizes.at(index)
            else
               res = 0
            end
            res
         end

         def supports_migrations?
            true
         end

         def prefetch_primary_key?(table_name = nil)
            true
         end

         def default_sequence_name(table_name, primary_key) # :nodoc:
            complete_trace(" in default_sequence_name(#{table_name}, #{primary_key}) ")
            "#{table_name} #{primary_key}"
         end

         def next_sequence_value(sequence_name)
            complete_trace(" in next_sequence_value ")

            ary = sequence_name.split(' ')
            if (!ary[1]) then
               ary[0] =~ /(\w+)_nonstd_seq/
               ary[0] = $1
               if( ary[1]== nil) then
                  ary[1]=get_primary_key
               end
            end

            # ary[0] is the table name (e.g. ary[0]=topics)
            # ary[1] is the index column (e.g. ary[1]=id)

            # @connection.unique_row_id(ary[0], ary[1])

            result_set = @connection.execute("select MAX ( #{ary[1]} ) FROM #{ary[0]} ")
            inner_result_set = result_set[0]
            max_id = inner_result_set[0]
            if (max_id==1) then
               # be sure this isn't an empty table
               # the code that returns ints can return a 1 if the number is
               # invalid or undefined. We want to be sure a 1 is really a 1
               table_count = @connection.execute("select COUNT (*) FROM #{ary[0]} ")
               table_count_inner_result_set = table_count[0]
               actual_table_count = table_count_inner_result_set[0]
               if(actual_table_count == 0) then
                  # the table is empty. Set the id field to be one
                  next_value = 1
               else
                  # the table isn't empty, which means the MAX id is really 1.
                  # that means the table has a single entry. We should set the next
                  # entry to be 2
                  next_value = 2
               end
            else
               max_id = 0 if max_id == "NULL"
               next_value = max_id + 1
            end
            if (next_value == nil) then
               puts "\n\n((((((((((((((( next_sequence_value returning nil \n\n"
            end
            if (next_value == 0) then
               puts "\n\n((((((((((((((( next_sequence_value returning 0 \n\n"
            end
            return next_value
         end


         # QUOTING ==================================================

         # COMMENT OUT following method for II_DATE_FORMAT=SWEDEN.
         #def quoted_date(value)
         # The following line is the original. It's from quoting.rb:
         #    value.strftime("%Y-%m-%d %H:%M:%S")
         # This fixed the date/time (US vs SWEDEN in II_DATE_FORMAT) issues -jrr
         #  value.strftime("%m-%d-%Y %H:%M:%S")
         #end

         def quote(value, column = nil)
            complete_trace(" in quote() ")
            if column
               complete_trace(" column= name::#{column.name}, @sql_type::#{column.sql_type}, @type=>#{value.class} ")
            end

            case value
              when String
                if column && column.type == :binary && column.class.respond_to?(:string_to_binary)
                  "'#{@connection.insert_binary(value)}'"
                else
                  "'#{quote_string(value)}'"
                end
              when TrueClass then '1'
              when FalseClass then '0'
              when Fixnum, Bignum then force_numeric?(column) ? value.to_s : "'#{value.to_s}'"
              when NilClass then "NULL"
              when Time, DateTime        then "'#{value.strftime("%Y-%m-%d %H:%M:%S")}'"
              else                       super
            end

         end

         def force_numeric?(column)
          (column.nil? || [:integer, :float, :decimal].include?(column.type))
         end

         # Quotes a string, escaping any ' (single quote) and \ (backslash)
         # characters.
         def quote_string(s)
            complete_trace(" in quote_string(#{s}) ")
            s.gsub(/'/, "''") # ' (for ruby-mode)
         end

         def quoted_true
            "1"
         end

         def quoted_false
            "0"
         end

         def quote_columns(column_name)
            complete_trace(" in quote_columns(#{column_name})")
            return column_name
         end

         # Quotes column names for use in SQL queries.
         def quote_column_name(name) #:nodoc:
            %("#{name}")
         end

         # DATABASE STATEMENTS ======================================

         # Ingres does not support ALTER TABLE RENAME COL so we have to do it another way
         #
         # !!!!Note!!!!
         # This method only handles tables and primary keys as generated by ActiveRecord
         # If someone has modified the table structure or added additional indexes these 
         # will be lost.
         # TODO - handle secondary indexes and alternate base table structures
         def rename_column(table_name, column_name, new_column_name) #:nodoc:
           table_columns = columns(table_name)
           #Strip the leading :
           old_column_name = "#{column_name}"
           no_table_columns = table_columns.size
           count = 0
           column_sql = ""
           table_columns.each do |col|
             count = count + 1 
             if col.name == old_column_name then
               column_sql << " #{col.name} as #{new_column_name}"
             else
               column_sql << " #{col.name}"
             end
             if (count < no_table_columns) then
               column_sql << ", " 
             end
           end
           pk_col = get_primary_key(table_name)
           # Backup the current table renaming the chosen column
           execute( <<-SQL_COPY, nil)
             DECLARE GLOBAL TEMPORARY TABLE session.table_copy AS
               SELECT #{column_sql}
               FROM #{table_name}
             ON COMMIT PRESERVE ROWS
             WITH NORECOVERY
           SQL_COPY

           #Drop table_name
           execute( <<-SQL_COPY, nil)
             DROP TABLE #{table_name}
           SQL_COPY

           #Re-create table_name based on session.table_copy
           execute( <<-SQL_COPY, nil)
             CREATE TABLE #{table_name} AS
               SELECT * FROM session.table_copy
           SQL_COPY

           #Add the Primary key back
           execute( <<-SQL_COPY, nil)
             ALTER TABLE #{table_name} ADD CONSTRAINT #{table_name[0..4]}_#{pk_col}_pk PRIMARY KEY (#{pk_col})
           SQL_COPY

           #Drop the GTT session.table_copy
           execute( <<-SQL_COPY, nil)
             DROP TABLE session.table_copy
           SQL_COPY
         end

         def add_column(table_name, column_name, type, options = {})
            complete_trace(" in add_column(#{table_name}, #{column_name}, #{type}, #{options}) ")

            add_column_sql = "ALTER TABLE #{table_name} ADD #{column_name} #{type_to_sql(type)}"
            if( (type.to_s=="string") && (options[:limit]!=nil) ) then
               add_column_sql.gsub!("varchar(255)", "varchar(#{options[:limit]})")
               # puts ("\ntype.to_s.class is #{type.to_s.class}")
            end
            execute(add_column_sql)
         end

         # Ingres can limit your number of replies using FIRST N
         # but it doesn't understand offset.
         # So I'm storing the values in @limit and ds_dataType@offset and
         # I'll apply the variables later in apply_limit_and_offset!

         def add_limit_offset!(sql, options) #:nodoc
            complete_trace(" in add_limit_offset!(#{sql}, #{options}) ")
            # reset the variables
            @limit=nil
            @offset=nil
            if @limit = options[:limit]
               unless @offset = options[:offset]
                  sql.gsub!("SELECT ", "SELECT FIRST #{@limit} ")
               end
            end
         end

         def apply_limit_and_offset!(result_set)
            complete_trace(" in apply_limit_and_offset!#{result_set}) ")
            # ingres doesn't have the concept of offset, so I've got to
            # apply the limit and offset here.
            # This is a horribly inefficient thing to do.
            # I'm not tracking the result set and returning a different bit
            # each time. I'm re-running the entire query.
            #
            # Note that just a limit can be applied using the FIRST
            # and it is in add_limit_offset!
            #
            if(1==2) then
               puts "\n=====----------==========--------=======----\n"
               puts "\n in apply_limit_and_offset!(result_set) "
               puts "\n limit=#{@limit} offset=#{@offset}\n"
               require 'pp'
               puts "result_set=#{pp result_set}\n"
               puts "\n=====----------==========--------=======----\n"
               puts result_set[0]
               puts result_set[1]

               puts "\n=====----------==========--------=======----\n"
            end
            if @offset then
               if @limit then
                  # if there's a limit requested, respect it
                  result_set = result_set[@offset , @limit]
               else
                  # otherwise, start at the offset and return the
                  # rest of the resultset
                  result_set = result_set[@offset, result_set.size]
               end
            end
            result_set
         end


         def select_all(sql, name = nil) #:nodoc:
            complete_trace(" in select_all(#{sql}, #{name}) ")
            execute_with_result_set(sql, name)
         end

         def select_one(sql, name = nil) #:nodoc:
            complete_trace(" in select_one(#{sql}, #{name}) ")
            add_limit_offset!(sql,{:limit => 1})
            results = select(sql, name)
            results.first if results
         end

         def select_rows(sql, name = nil)
            complete_trace(" in select_rows(#{sql}, #{name}) ")
            result = select_all(sql, name)
            rows = []
            result.each do |row| 
               rows << row.values_at("version")
            end
            rows
         end

         def insert(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil) #:nodoc:
            complete_trace(" in insert(#{sql}, #{name}, #{pk}, #{id_value}, #{sequence_name}) ")
            execute(sql, name)
            update_nulls_after_insert(sql, name, pk, id_value, sequence_name)
            id_value
         end

         def exception_str(exc=$!)
            complete_trace(" in execption_str(#{exc}) ")

            str = ''
            first = true
            exc.backtrace.each do |bt|
               str << (first \
               ? "#{bt}: #{exc.message} (#{exc.class})\n" \
               : "\tfrom #{bt}\n")
               first = false
            end
            str
         end

         def print_exception(exc=$!, out=$stdout)
            out.puts exception_str(exc)
         end


         def my_print_exception(msg)
            begin
               raise msg
            rescue
               print_exception()
            end
         end


         def update(sql, name = nil) #:nodoc:
            complete_trace(" in update(#{sql}, #{name}) ")
            execute(sql, name)
            return @connection.rows_affected
         end

         def delete(sql, name = nil) #:nodoc:
            complete_trace(" in delete(#{sql}, #{name} ")
            execute(sql, name)
            return @connection.rows_affected
         end


         def execute(sql, name = nil) #:nodoc:
            complete_trace(" in execute(#{sql}, #{name}) ")

            # debug print
            if(1==2) then
               bad_sql = "auto_id_tests"
               if(sql.include?bad_sql) then
                  my_print_exception(bad_sql)
               end
            end

            sql = add_ordering(sql)
            #sql = sql.gsub("\n", "\\n")

            begin
               result = log(sql, name) { @connection.execute(sql) }
            rescue
               puts "\nAn error occurred!\n"
               print_exception()
            end
            return result
         end

         def execute_with_result_set(sql, name = nil) #:nodoc:
            complete_trace(" in execute_with_result_set(#{sql}, #{name}) ")

            sql = add_ordering(sql)
            rows = execute_without_adding_ordering(sql)
            return rows
         end

         def execute_without_adding_ordering(sql, name=nil)
            complete_trace(" in execute_without_adding_ordering(#{sql}, #{name}) ")

            # TODO: clean up this hack
            sql.gsub!(" = NULL", " is NULL")

            # debug print
            if(1==2) then
               bad_sql = "auto_id_tests"
               if(sql.include?bad_sql) then
                  my_print_exception(bad_sql)
               end
            end

            begin
               rs = log(sql, name) { @connection.execute(sql) }
            rescue
               puts "\nAn error occurred!\n"
               print_exception()
            end
            col_names = @connection.column_list_of_names
            data_type = @connection.data_types

            rows=[]
            rs.each do |row|
               index = 0
               this_column = Hash.new
               row.each do |item |

                  col_name = col_names[index].rstrip
                  col_val = item.to_s.rstrip

                  if (col_val=="NULL") then
                     col_val = nil
                  end
                  this_column[ col_name] = col_val
                  index += 1
               end
               rows << this_column
               index = 0
            end
            if(@offset) then
               rows = apply_limit_and_offset!(rows)
            end

            # debug print block
            if(1==2) then
               puts "\n=============\n"
               puts "the result set is:\n"
               pp rows
               puts "\n=============\n"
            end

            if(1==2 && rows[0] && rows[0].values_at("content")==["--- \n- one\n- two\n- three"]) then
               # h = rows[0]
               # h.delete("content")
               # h.store("content", "--- \n- one\n- two\n- three\n")
               # rows[0] = h
            end

            return rows
         end


         # I'm redefining the remove_index routine here.
         # The original is in schema_statements.rb

         def remove_index(table_name, options = {})
            complete_trace(" in remove_index(#{table_name}, #{options}) ")
            execute "DROP INDEX #{index_name(table_name, options)}"
         end

         alias_method :delete, :update #:nodoc:
         #=begin
         def begin_db_transaction #:nodoc:
            execute "START TRANSACTION"
         rescue Exception
            # Transactions aren't supported
         end

         def commit_db_transaction #:nodoc:
            execute("COMMIT")
         rescue Exception
            # Transactions aren't supported
         end

         def rollback_db_transaction #:nodoc:
            execute "ROLLBACK"
         rescue Exception
            # Transactions aren't supported
         end
         #=end

         # SCHEMA STATEMENTS ========================================

         # Create an Ingres database
         def create_database(name, options = {})
           #`createdb #{name} -i -fnofeclients`
         end

         # Drops an Ingres database
         def drop_database(name) #:nodoc:
           #`destroydb #{name}`
         end

         # Return the list of all tables in the schema search path.
         def tables(name = nil) #:nodoc:
            complete_trace(" in tables(#{name}) ")
            tables = @connection.tables
            #tables.reject! { |t| /\A_SYS_/ === t }
            tables
         end

         def columns(table_name, name = nil) #:nodoc:
            complete_trace(" in columns(#{table_name}, #{name}) ")

            sql = "SELECT column_name, column_default_val, column_datatype, column_length FROM iicolumns  "
            sql << "WHERE table_name='#{table_name}' "
            sql << "ORDER BY column_sequence "
            columns = []
            select_all(sql, name).each do |row|
               default_value = default_value( row["column_default_val"])
               column_type = sql_type_name( row["column_datatype"], row["column_length"])

               if (1==2) then
                  puts "\n---\n"
                  puts row["column_name"]
                  puts default_value
                  puts column_type
               end
               data_size = get_data_size(row["column_name"])

               complete_trace ("-----------------------")
               complete_trace ("row['column_name']=#{row["column_name"]}")
               complete_trace ("default_value=#{default_value}")
               complete_trace ("column_type=#{column_type}"      )
               complete_trace ("data_size=#{data_size}"  )
               complete_trace ("-----------------------")

               column_type_hash = { "data_size" => data_size, "column_type" => column_type}

               columns << IngresColumn.new(row["column_name"],
               default_value,
               column_type_hash,
               row["notnull"]
               )
            end
            columns
         end

         # Ingres 9.1.x and earlier require 'COLNAME TYPE DEFAULT NULL WITH NULL' as part of the column definition
         # for CREATE TABLE
         # TODO : add a server version check so as to only do this for Ingres 9.1.x and earlier.
         def add_column_options!(sql, options) #:nodoc:
            sql << " DEFAULT #{quote(options[:default], options[:column])}" if options_include_default?(options)
            # must explcitly check for :null to allow change_column to work on migrations
            if options.has_key? :null
               if options[:null] == false
                  sql << " NOT NULL"
               else
                  sql << " WITH NULL"
               end
            end
         end

         def get_ordering_field(table_name)
            field = get_primary_key(table_name)
            if(!field) then
               # fetch some field from the table (or sql) to order
               # if need be, fetch another field here to order by...
               # get a list of all fields that are indexed (key_sequence !=0)
               sql = "SELECT column_name, column_length FROM iicolumns WHERE table_name = '#{table_name}' AND key_sequence != 0 ORDER BY column_name"
               result_set = execute_without_adding_ordering(sql)

               if(result_set==nil || result_set==[]) then

                  field = nil
                  # didn't get a hit... cast a broader net and just pick a table
                  sql = "SELECT column_name, column_length FROM iicolumns WHERE table_name = '#{table_name}' ORDER BY column_name"
                  result_set = execute_without_adding_ordering(sql)

               else
                  # now verify that our field is in the result set

                  # breakpoint =-==
                  # TODO: finish this!
                  # result_set.flatten.include?(


               end
               if(!field) then
                  # just take the first field name we see.
                  # this table has no primary key and no indexes
                  # we need some sort of ordering to be consistent, so we're using the
                  # first table we see, alphabetically
                  begin
                     if(result_set == [] || result_set ==nil) then
                        field = nil
                     else
                        field  = result_set[0][0]
                     end
                  rescue
                     breakpoint
                     field = nil
                  end
               end
            else
               # should we validate that this field exists in the table we are executing against
               # if it doesn't, pick a replacement
            end

            return field
         end

         def get_primary_key(table_name)
            complete_trace(" in get_primary_key(#{table_name} ")
            indexes(table_name)
            return @primary_key
         end

         def indexes(table_name, name = nil)#:nodoc:
            complete_trace(" in indexes(#{table_name}, #{name}) ")
            @primary_key = nil
            sql =  "SELECT base_name, index_name, unique_rule "
            sql <<    "FROM "
            sql <<    "iiindexes "
            sql <<    "WHERE  "
            sql <<    "base_name = '#{table_name}' "

            indexes = []
            result_set = execute_without_adding_ordering(sql, name)

            if(result_set) then
               result_set.each do | column |
                  base_name  = column.values_at("base_name")[0]
                  index_name = column.values_at("index_name")[0]
                  unique_rule =column.values_at("unique_rule")[0]=='U'
                  column_name = column.values_at("column_name")[0]

                  # get a copy of the  primary index
                  # Ingres uses the first 5 characters of a table name for the primary key index
                  if table_name.length < 5 then
                    short_name = table_name
                  else
                    short_name = table_name[0..4]
                  end
                  if (index_name.starts_with?("$#{short_name}")) then
                    # we have the name of the pointer to the index
                    # now let's fetch the actual name of the primary key
                    sql2="select column_name from iiindex_columns where index_name='#{index_name}'"
                    result_set_2 = execute_without_adding_ordering(sql2)
                    @primary_key = result_set_2[0].fetch('column_name')
                  end

                  # ignore any index with a $pk or ii prefix. It's a system index
                  prefix1 = "$#{short_name}"
                  prefix2 = "ii"
                  if ( (index_name.starts_with?(prefix1)) || (index_name.starts_with?(prefix2)) ) then
                     #puts "\nSkipping #{index_name}\n"
                  else
                     #puts "\nAdding >>#{index_name}<<\n"

                     # now fetch the column names and store them in an Array
                     column_names = []
                     sql = "SELECT column_name from iiindex_columns "
                     sql << " WHERE index_name = '#{index_name}' "
                     rs2 = execute_without_adding_ordering(sql, name)
                     rs2.each do | col2 |
                        this_col_name = col2.values_at("column_name").first
                        column_names << this_col_name
                     end
                     # class IndexDefinition < Struct.new(:table, :name, :unique, :columns) #:nodoc:
                     indexes << IndexDefinition.new( table_name, index_name, unique_rule, column_names)
                     complete_trace( "Just created an index with (#{table_name}, #{index_name}, #{unique_rule}, #{column_names})\n")
                  end
               end
            end
            indexes
         end


         private
         def select(sql, name = nil)
            complete_trace(" in select(#{sql}, #{name}) ")

            sql = translate_sql(sql)
            results = execute_with_result_set(sql, name)

            rows = []
            counter = 0
            if ( @connection.rows_affected )
               results.each do |row|  # loop through result rows
                  hashed_row = {}

                  row.each_pair do |key, value|
                     hashed_row[key] = value unless key == "id"
                  end
                  rows << hashed_row
               end
            end
            if(1==2) then
               puts "rows.class=#{rows.class}\n"
               puts "\n\n rows is: \n"
               rows.each { |x| x.each {|y| puts y } }
            end

            rows
         end

         def default_value(value)
            complete_trace(" in default_value#{value}) ")
            # Empty strings should be set to null
            return nil if value == nil
            return nil if value.empty?

            # Boolean type values
            return true if value =~ /true/
            return false if value =~ /false/

            # Date / Time magic values
            # return Time.now.to_s if value =~ /^now\(\)/i

            # Otherwise return what we got from the database
            # and hope for the best..
            complete_trace(" Exiting default_value... defaulting to #{value}) ")
            return value
         end

         def sql_type_name(type_name, length)
            complete_trace(" in sql_type_name(#{type_name}, #{length}) ")
            if(2==3) then
               puts "\n In sql_type_name \n"
               puts "\n-------------\n"
               puts "type_name=#{type_name} and length=(#{length})"
               puts "\n-------------\n"
            end

            if( (type_name=="INTEGER") && (length.to_s=="1") ) then
               return "BOOLEAN"
            end


            #if(type_name=="VARCHAR" && length==255)
            if(2==3)
               then
                  my_print_exception("in sql_type_name")
               end
               return "#{type_name}(#{length})" if ( type_name =~ /char/ )
               type_name
            end

            def ingres_index_name(row = [])
               complete_trace(" in ingres_index_name#{row}) ")

               name = ""
               name << "UNIQUE " if row[3]
               name << "CLUSTERED " if row[4]
               name << "INDEX"
               name
            end

            def translate_sql(sql)
               complete_trace(" in translate_sql(#{sql}) ")
               # Change table.* to list of columns in table
               while (sql =~ /SELECT.*\s(\w+)\.\*/)
                  table = $1
                  cols = columns(table)
                  if ( cols.size == 0 ) then
                     # Maybe this is a table alias
                     sql =~ /FROM(.+?)(?:LEFT|OUTER|JOIN|WHERE|GROUP|HAVING|ORDER|RETURN|$)/
                     $1 =~ /[\s|,](\w+)\s+#{table}[\s|,]/ # get the tablename for this alias
                     cols = columns($1)
                  end
                  select_columns = []
                  cols.each do |col|
                     select_columns << table + '.' + col.name
                  end
                  sql.gsub!(table + '.*',select_columns.join(", ")) if select_columns
               end

               # Change JOIN clause to table list and WHERE condition

               while (sql =~ /JOIN/)
                  sql =~ /((LEFT )?(OUTER )?JOIN (\w+) ON )(.+?)(?:LEFT|OUTER|JOIN|WHERE|GROUP|HAVING|ORDER|RETURN|$)/
                  join_clause = $1 + $5
                  is_outer_join = $3
                  join_table = $4
                  join_condition = $5
                  #join_condition.gsub!(/=/,"*") if is_outer_join
                  if (sql =~ /WHERE/)
                     sql.gsub!(/WHERE/,"WHERE (#{join_condition}) AND")
                  else
                     sql.gsub!(join_clause,"#{join_clause} WHERE #{join_condition}")
                  end
                  sql =~ /(FROM .+?)(?:LEFT|OUTER|JOIN|WHERE|$)/
                  from_clause = $1
                  sql.gsub!(from_clause,"#{from_clause}, #{join_table} ")
                  sql.gsub!(join_clause,"")
               end
               sql = add_ordering(sql)
               sql
            end

            # Ingres tends to return data in a different format depending
            # on what queries were previously run, so we need to explicitly
            # order all selects

            def add_ordering (sql)
               complete_trace(" in add_ordering(#{sql}) ")
               if(sql !~ /(SELECT)|(select)/)
                  return sql
               end

               # extract the next word after FROM... it's the table name
               from_stmnt = sql.scan(/(FROM \w+)|(from \w+)/)
               table_array = from_stmnt.to_s.scan(/\w+/)
               table_name = table_array[1]

               ordering_field = get_ordering_field(table_name)
               if(ordering_field==nil) then
                  return sql
               end

               # ORDER BY ordering_field if no explicit ORDER BY
               # This will ensure that find(:first) returns the first inserted row
               # or at least the same value every time
               sql_original = sql
               sql_temp = sql
               if(sql_temp =~ /(SELECT)/) then
                  if (sql_temp.upcase !~ /(COUNT)|(MAX)|(MIN)|(AVG)|(ANY)|(SUM)|(TRANSACTION)|(ROLLBACK)|(COMMIT)|(JOIN)/) then
                     if (sql_temp !~ /(ORDER BY)|(GROUP BY)/)
                        if (sql =~ /RETURN RESULTS 1/)
                           sql.sub!(/RETURN RESULTS 1/,"ORDER BY #{ordering_field}")
                           #sql.sub!(/RETURN RESULTS 1/," ")
                        else
                           sql << " ORDER BY #{ordering_field}"
                        end
                        # Ingres requires that anything you order by be in the select
                        # TODO: ADDRESS CASE ISSUES
                        if (sql =~ /(#{ordering_field})/ ) then

                           # the original query didn't have an ID in it. Let's add one
                           # unless it's already got a "SELECT *"
                           if (sql !~ /(FIRST)/) then
                              # insert ordering_field into the select list
                              found_select_star = sql.gsub("SELECT *", "I found it!")
                              # TODO: Is this right? It reads backwards (if it's working)
                              # retool to read properly -jrr
                              if (sql==found_select_star) then
                                 sql.gsub!("SELECT", "SELECT #{ordering_field}, ")
                              end
                           else
                              # we have a query with a "FIRST N" in it.
                              # ID needs to go after the first.
                              # example: "SELECT FIRST 7 id, name from COMPANIES.... "

                              # skip the replacement if the select has a * in it
                              found_select_star = sql.gsub("*", "I found it!")
                              if (sql==found_select_star) then
                                 sql.gsub!( /(FIRST \d+ )/, '\1 #{ordering_field}, ')
                              end
                           end
                        end
                     end
                  end
               end
               #puts "\n sql coming exiting is:\n#{sql}\n\n"
               sql
            end

            def update_nulls_after_insert(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil)
               complete_trace(" in update_nulls_after_insert( #{sql}, #{name}, #{pk}, #{id_value}, #{sequence_name}) ")
               # TODO: is this okay? :(

               if(1) then return end
               puts "\n In update_nulls_after_insert\n"

               sql =~ /INSERT INTO (\w+) \((.*)\) VALUES\s*\((.*)\)/m
               table = $1
               cols = $2
               values = $3
               cols = cols.split(',')
               values.gsub!(/'[^']*'/,"''")
               values.gsub!(/"[^"]*"/,"\"\"")
               values = values.split(',')
               update_cols = []
               values.each_index { |index| update_cols << cols[index] if values[index] =~ /\s*NULL\s*/ }
               update_sql = "UPDATE #{table} SET"
               update_cols.each { |col| update_sql << " #{col}=NULL," unless col.empty? }
               update_sql.chop!()
               update_sql << " WHERE #{pk}=#{quote(id_value)}"
               execute(update_sql, name + " NULL Correction") if update_cols.size > 0
            end
         end
      end
   end

#vim: ts=2 sw=2 expandtab
