require 'active_record/connection_adapters/abstract_adapter'
require 'active_support/core_ext/object/blank'
require 'active_record/connection_adapters/statement_pool'
require 'arel/visitors/bind_visitor'

module ActiveRecord
  class Base
    # Establishes a connection to the database that's used by all Active Record objects
    def self.ingres_connection(config) # :nodoc:
      require 'Ingres' unless self.class.const_defined?(:Ingres)

      config = config.symbolize_keys
      host = config[:host]
      port = config[:port] || 21064
      username = config[:username].to_s if config[:username]
      password = config[:password].to_s if config[:password]

      if config.key?(:database)
        database = config[:database]
      else
        raise ArgumentError, "No database specified. Missing argument: database."
      end

      #return the connection adapter
      ConnectionAdapters::IngresAdapter.new(nil, logger, [host, port, nil, nil, database, username, password], config)
    end
  end

  module ConnectionAdapters
    class IngresColumn < Column #:nodoc:
      def initialize(name, default, sql_type = nil, null = true)
        super(name, default, sql_type, null)
      end

      def value_to_boolean(str)
        return (str.to_i == 0 ? 0 : 1)
      end

      def type_cast(value)
        return nil if value.nil?
        case @type
        when :string    then value
        when :text      then value
        when :float     then value.to_f
        when :decimal   then self.class.value_to_decimal(value)
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
        case type
        when :string    then nil
        when :text      then nil
        when :integer   then "(#{var_name}.to_i rescue #{var_name} ? 1 : 0)"
        when :float     then "#{var_name}.to_f"
        when :decimal   then "#{self.class.name}.value_to_decimal(#{var_name})}"
        when :datetime  then "#{self.class.name}.string_to_time(#{var_name})"
        when :timestamp then "#{self.class.name}.string_to_time(#{var_name})"
        when :time      then "#{self.class.name}.string_to_dummy_time(#{var_name})"
        when :binary    then "#{self.class.name}.binary_to_string(#{var_name})"
        when :boolean   then "#{self.class.name}.value_to_boolean(#{var_name})"
        when :date
          str = "if(@name =~ /_time$/ ) then \n"
          str << " #{var_name}=#{var_name}[11,19] \n"
          str << " value = '2000-01-01' << #{var_name} \n"
          str << " res = #{self.class.name}.string_to_time(#{var_name}) \n"
          str << " else \n"
          str << " res = #{self.class.name}.string_to_date(#{var_name}) \n"
          str << " end\n"
          str
        else nil
        end
      end


      private

      def extract_limit(sql_type)
        case sql_type
        when /^bigint/i;    8
        when /^smallint/i;  2
        else super
        end
      end

      def simplified_type(field_type)
        case field_type
        when /INTEGER/
          :integer
        when /FLOAT/
          :float
        when /DECIMAL/
          extract_scale(field_type) == 0 ? :integer : :decimal
        when /MONEY/
          :decimal
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
        else
          super
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
    # Author: jared@jaredrichardson.net
    # Maintainer: bruce.lunsford@ingres.com
    # Maintainer: grant.croker@ingres.com
    #
    class IngresAdapter < AbstractAdapter

      ADAPTER_NAME = 'Ingres'

      NATIVE_DATABASE_TYPES = {
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
        :boolean     => { :name => "tinyint" },
        :decimal     => { :name => "decimal" }
      }

      def adapter_name
        ADAPTER_NAME
      end

      class StatementPool < ConnectionAdapters::StatementPool
        def initialize(connection, max = 1000)
          super
          @cache   = Hash.new { |h,pid| h[pid] = {} }
        end

        def each(&block); cache.each(&block); end
        def key?(key);    cache.key?(key); end
        def [](key);      cache[key]; end
        def length;       cache.length; end
        def delete(key);  cache.delete(key); end

        def []=(sql, key)
          while @max < cache.size
            cache.shift.last[:stmt].close
          end
          cache[sql] = key
        end

        def clear
          cache.values.each do |hash|
            hash[:stmt].close
          end
          cache.clear
        end

        private
        def cache
          @cache[$$]
        end
      end

      class BindSubstitution < Arel::Visitors::Ingres # :nodoc:
        include Arel::Visitors::BindVisitor
      end

      def initialize(connection, logger, connection_parameters, config)
        super(connection, logger)

        if config.fetch(:prepared_statements) { true }
          @visitor = Arel::Visitors::Ingres.new self
        else
          @visitor = BindSubstitution.new self
        end

        @connection_parameters, @config = connection_parameters, config

        connect
        @statements = StatementPool.new(@conection,
                                        config.fetch(:statement_limit) { 1000 })
      end

      def native_database_types
        NATIVE_DATABASE_TYPES
      end

      # Ingres supports a different SQL syntax for column type definitions
      # For example with other vendor DBMS engines it's possible to specify the
      # number of bytes in an integer column - e.g. INTEGER(3)
      #
      # In addition it would appear that the following syntax is not valid for Ingres
      # colname DECIMAL DEFAULT xx.yy
      # where as
      # colname DECIMAL
      # is valid. It would appear we need to supply the precision and scale when providing
      # a default value.
      def type_to_sql(type, limit = nil, precision = nil, scale = nil)
        case type.to_sym
        when :integer;
          case limit
          when 1..2;      return 'smallint'
          when 3..4, nil; return 'integer'
          when 5..8;      return 'bigint'
          else raise(ActiveRecordError, "No integer type has byte size #{limit}. Use a numeric with precision 0 instead.")
          end
        when :boolean; return 'tinyint'
        when :decimal;
          if precision.nil? then
            # Ingres requires the precision/scale be defined
            return 'decimal (39,15)'
          else
            return "decimal (#{precision},#{scale})"
          end
        else
          return super
        end
      end

      # Fetch the column used as a primary key in the join
      # TODO Handle a primary key added using ALTER TABLE
      def primary_key(table)
        sql =  "SELECT column_name "
        sql << "FROM iicolumns, iiconstraints "
        sql << "WHERE iiconstraints.table_name = '#{table}' "
        sql << "AND   iiconstraints.constraint_name = iicolumns.table_name " 
        sql << "AND   iiconstraints.constraint_type = 'P' "
        sql << "AND   iicolumns.column_name != 'tidp' "
        sql << "ORDER BY iicolumns.column_sequence "
        primary_key = @connection.execute(sql).first
        primary_key = "id" if primary_key == nil
        return primary_key
      end

      def get_data_size(id)
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

      # Can this adapter determine the primary key for tables not attached
      # to an ActiveRecord class, such as join tables?  Backend specific, as
      # the abstract adapter always returns +false+.
      def supports_primary_key?
        true
      end

      # Does this adapter support DDL rollbacks in transactions?  That is, would
      # CREATE TABLE or ALTER TABLE get rolled back by a transaction?
      def supports_ddl_transactions?
        true
      end

      # Does this adapter support savepoints?
      # We do but there is no delete/release savepoint feature which
      # is needed by ActiveRecord
      def supports_savepoints?
        false
      end

      def supports_migrations?
        true
      end

      # Should primary key values be selected from their corresponding
      # sequence before the insert statement?  If true, next_sequence_value
      # is called before each insert to set the record's primary key.
      def prefetch_primary_key?(table_name = nil)
        true
      end

      def default_sequence_name(table_name, primary_key) # :nodoc:
        "#{table_name} #{primary_key}"
      end

      def next_sequence_value(sequence_name)

        ary = sequence_name.split(' ')

        if use_table_sequence? then
          next_value = next_identity_for_table(ary[0])
        else
          if (!ary[1]) then
            ary[0] =~ /(\w+)_nonstd_seq/
            ary[0] = $1
            if( ary[1]== nil) then
              last_identity = last_identity_for_table($1) 
            end
          else
            last_identity = last_identity_for_table(ary[0]) 
          end
          if last_identity == "NULL"
            next_value = 1
          else
            next_value = last_identity + 1
          end
        end
        next_value
      end

      # Should we use sequence defined in the default value
      def use_table_sequence?
        if @config.has_key? :use_sequence_for_identity then
          return @config[:use_sequence_for_identity] 
        else
          return FALSE
        end
      end

      # QUOTING ==================================================

      def quote(value, column = nil)
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
        s.gsub(/'/, "''") # ' (for ruby-mode)
      end

      def quoted_true
        "1"
      end

      def quoted_false
        "0"
      end

      def quote_columns(column_name)
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
        pk_col = primary_key(table_name)
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

      def select_rows(sql, name = nil)
        result = select_all(sql, name)
        rows = []
        result.each do |row| 
          rows << row.values_at("version")
        end
        rows
      end

      def insert_sql(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil) #:nodoc:
        execute sql, name
        id_value
      end

      def exception_str(exc=$!)
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
        execute(sql, name)
        return @connection.rows_affected
      end

      def delete(sql, name = nil) #:nodoc:
        execute(sql, name)
        return @connection.rows_affected
      end

      # Returns the last auto-generated ID from the affected table.
      def insert(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil)
        insert_sql(sql, name, pk, id_value, sequence_name)
      end

      def execute(sql, name = nil) #:nodoc:
        # debug print
        if(1==2) then
          bad_sql = "auto_id_tests"
          if(sql.include?bad_sql) then
            my_print_exception(bad_sql)
          end
        end

        sql = add_ordering(sql)

        # Ingres does not like UPDATE statements that have a sub-SELECT that uses
        # an ORDER BY
        if sql =~ /^UPDATE/i then
          sql.gsub!(/( ORDER BY.*)\)/,")") if sql =~ /SELECT.*( ORDER BY.*)\)/i 
        end

        begin
          result = log(sql, name) { @connection.execute(sql) }
        rescue
          puts "\nAn error occurred!\n"
          print_exception()
        end
        return result
      end

      def execute_with_result_set(sql, name = nil, binds = []) #:nodoc:
        sql = add_ordering(sql)
        rows = execute_without_adding_ordering(sql)
        return rows
      end

      def execute_without_adding_ordering(sql, name=nil, binds = {})
        # TODO: clean up this hack
        sql.gsub!(" = NULL", " is NULL")
        sql.gsub!(" IN (NULL)", " is NULL")

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

        return rows
      end


      # I'm redefining the remove_index routine here.
      # The original is in schema_statements.rb

      def remove_index(table_name, options = {})
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
        tables = @connection.tables.flatten
        tables
      end

      def columns(table_name, name = nil) #:nodoc:

        sql = "SELECT column_name, column_default_val, column_datatype, column_length FROM iicolumns  "
        sql << "WHERE table_name='#{table_name}' "
        sql << "ORDER BY column_sequence "
        columns = []
        execute_without_adding_ordering(sql, name).each do |row|
          default_value = default_value( row["column_default_val"])
          column_type = sql_type_name( row["column_datatype"], row["column_length"])

          if (1==2) then
            puts "\n---\n"
            puts row["column_name"]
            puts default_value
            puts column_type
          end
          data_size = get_data_size(row["column_name"])

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

        field = primary_key(table_name)

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
        indexes(table_name)
        return @primary_key
      end

      def indexes(table_name, name = nil)#:nodoc:
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
            end
          end
        end
        indexes
      end


      private

      def connect
        @connection = Ingres.new

        @connection.connect({
          :host        => @connection_parameters[0],
          :port        => @connection_parameters[1],
          :database    => @connection_parameters[4],
          :username    => @connection_parameters[5],
          :password    => @connection_parameters[6],
          :date_format => Ingres::DATE_FORMAT_FINLAND
        })

        configure_connection
      end

      def configure_connection
        # Dummy function to execute further configuration on connection
      end

      def select(sql, name = nil, binds = [])
        sql = translate_sql(sql)
        results = execute_with_result_set(sql, name)

        rows = []
        counter = 0
        if (@connection.rows_affected)
          results.each do |row|  # loop through result rows
            hashed_row = {}

            row.each_pair do |key, value|
              hashed_row[key] = value unless key == "id"
            end
            rows << hashed_row
          end
        end

        if (1==2) then
          puts "rows.class=#{rows.class}\n"
          puts "\n\n rows is: \n"
          rows.each { |x| x.each {|y| puts y } }
        end

        rows
      end

      def default_value(value)
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
        return value
      end

      def sql_type_name(type_name, length)
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
        name = ""
        name << "UNIQUE " if row[3]
        name << "CLUSTERED " if row[4]
        name << "INDEX"
        name
      end

      def translate_sql(sql)
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
        if(sql !~ /^(SELECT)/i)
          return sql
        end

        # extract the next word after FROM... it's the table name
        from_stmnt = sql.scan(/(FROM \w+)|(from \w+)/)
        table_array = from_stmnt.to_s.scan(/\w+/)
        table_name = table_array[1]

        # Get the primary key column name if we have a table to look at
        ordering_field = get_ordering_field(table_name) if table_name != nil
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
                    # we need to handle table aliases of the form
                    # FROM table t1
                    # for the time being this is commented out as it causes 
                    # test_query_attribute_with_custom_fields to fail as it contains
                    # custom SQL
                    #sql.gsub!("SELECT", "SELECT #{table_name}.#{ordering_field}, ")
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

      # Get the last generate identity/auto_increment/sequence number generated
      # for a given table
      def last_identity_for_table(table)
        if table != nil then
          identity_col = primary_key(table)
          sql = "SELECT max(#{identity_col}) "
          sql << "FROM #{table}"
          last_identity = @connection.execute(sql)[0][0]
        else
          last_identity = 0
        end
        last_identity
      end

      def next_identity_for_table(table)
        if table != nil then
          identity_col = primary_key(table)
          if identity_col != nil then
            sequence_name = table_sequence_name(table,identity_col)
            if sequence_name != nil
              sql = "SELECT #{sequence_name}.nextval"
              next_identity = @connection.execute(sql)[0][0]
              # Test for a value which is <= the max value already there
              # to avoid possible duplicate key values
              sql = "SELECT max(#{identity_col}) from #{table}"
              max_id = @connection.execute(sql)[0][0]
              max_id = 0 if max_id == "NULL"
              until next_identity > max_id
                sql = "SELECT #{sequence_name}.nextval"
                next_identity = @connection.execute(sql)[0][0]
              end
              @identity_value = next_identity
            else
              next_identity = last_identity_for_table(table) + 1
            end
          else
            next_identity = 1
          end
        else
          next_identity = 1
        end
        next_identity
      end

      # Get the last generate identity/auto_increment/sequence number generated
      # for a given insert statement
      def last_identity_for_insert(sql)
        puts "In last_identity_for_insert()"
        sql =~ /INSERT INTO "(\w+)" .*/m 
        last_identity_for_table($1)
      end

      # Get the sequence name used in the table
      def table_sequence_name(table, column)
        sql = "SELECT column_default_val "
        sql << "FROM iicolumns "
        sql << "WHERE table_name = '#{table}' "
        sql << "  AND column_name = '#{column}'"
        default = @connection.execute(sql)[0][0]
        default =~ /next value for "(\w+)"\."(\w+)"/m
        sequence_name = $2
      end

      def update_nulls_after_insert(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil)
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
