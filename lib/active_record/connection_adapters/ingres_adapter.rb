require 'active_record/connection_adapters/abstract_adapter'
require 'active_support/core_ext/object/blank'
require 'active_record/connection_adapters/statement_pool'

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
      def initialize(name, default, sql_type = nil, null = true, default_function = nil)
        super(name, default, sql_type, null)

        @default_function = default_function
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

      ADAPTER_NAME = 'Ingres'.freeze

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
      }.freeze

      PARAMETERS_TYPES = {
        "byte"         => "b",
        "long_byte"    => "B",
        "char"         => "c",
        "date"         => "d",
        "decimal"      => "D",
        "float"        => "f",
        "integer"      => "i",
        "nchar"        => "n",
        "nvarchar"     => "N",
        "text"         => "t",
        "long_text"    => "T",
        "varchar"      => "v",
        "long_varchar" => "V",
      }.freeze

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

      def initialize(connection, logger, connection_parameters, config)
        super(connection, logger)

        if config.fetch(:prepared_statements) { true }
          @visitor = Arel::Visitors::Ingres.new self
        else
          @visitor = BindSubstitution.new self
        end

        @connection_parameters, @config = connection_parameters, config

        connect
        @statements = StatementPool.new(@connection,
                                        config.fetch(:statement_limit) { 1000 })
      end

      def adapter_name
        ADAPTER_NAME
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

      # CONNECTION MANAGEMENT ====================================

      def active?
        @connection.exec 'SELECT 1'
        true
      rescue Exception
        false
      end

      def reconnect!
        disconnect!
        clear_cache!
        connect
      end

      def reset!
        clear_cache!
        super
      end

      def disconnect!
        @connection.disconnect rescue nil
      end

      def native_database_types
        NATIVE_DATABASE_TYPES
      end

      # HELPER METHODS ===========================================

      def new_column(column_name, default, type)
        IngresColumn.new(column_name, default, type)
      end

      # QUOTING ==================================================

      def force_numeric?(column)
        (column.nil? || [:integer, :float, :decimal].include?(column.type))
      end

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

      # Quotes a string, escaping any ' (single quote) and \ (backslash)
      # characters.
      def quote_string(s)
        s.gsub(/'/, "''") # ' (for ruby-mode)
      end

      # Quotes column names for use in SQL queries.
      def quote_column_name(name) #:nodoc:
        %("#{name}")
      end

      # DATABASE STATEMENTS ======================================

      def clear_cache!
        @statements.clear
      end

      def exec_query(sql, name = 'SQL', binds = [])
        log(sql, name, binds) do
          #TODO Aiming to do prepared statements but we'll have to do changes to the C API
          #result = binds.empty? ? exec_no_cache(sql, binds) :
          #                        exec_cache(sql, binds)
          result = binds.empty? ? @connection.execute(sql) :
                                  @connection.execute(sql, *binds.map { |bind| [PARAMETERS_TYPES[bind[0].sql_type.downcase], bind[1]] }.flatten)

          if @connection.rows_affected
            # Dirty hack for ASCII-8BIT strings
            result.each do |row|
              row.each_with_index do |column, index|
                if String === column && column.encoding == Encoding::ASCII_8BIT
                  row[index] = column.unpack("C*").pack("U*")
                end
              end
            end

            ActiveRecord::Result.new(@connection.column_list_of_names, result)
          else
            ActiveRecord::Result.new([], [])
          end
        end
      end

      def exec_delete(sql, name = 'SQL', binds = [])
        exec_query(sql, name, binds)
        @connection.rows_affected
      end
      alias :exec_update :exec_delete

      # Get the last generate identity/auto_increment/sequence number generated
      # for a given table
      def last_inserted_id(table)
        r = exec_query("SELECT max(#{primary_key(table)}) FROM #{table}")
        Integer(r.first.first)
      end

      def execute(sql, name = nil)
        log(sql, name) do
          @connection.execute(sql)
        end
      end

      def insert_sql(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil) #:nodoc:
        super
        id_value
      end
      alias :create :insert_sql

      def select_rows(sql, name = nil)
        execute(sql, name).to_a
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

      def execute_without_adding_ordering(sql, name=nil)
        # TODO: clean up this hack
        sql.gsub!(" = NULL", " is NULL")
        sql.gsub!(" IN (NULL)", " is NULL")

        begin
          rs = @connection.execute(sql)
        rescue
          puts "\nAn error occurred!\n"
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

      def begin_db_transaction #:nodoc:
        execute "START TRANSACTION"
      rescue Exception
        # Transactions aren't supported
      end

      def commit_db_transaction #:nodoc:
        execute "COMMIT"
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

      def columns(table_name, name = nil) #:nodoc:
        column_definitions(table_name).collect do |row|
          new_column(row["column_name"], row["column_default_val"], row["column_datatype"])
        end
      end

      # Returns just a table's primary key
      def primary_key(table)
        row = @connection.execute(<<-end_sql).first
          SELECT column_name
          FROM iicolumns, iiconstraints
          WHERE iiconstraints.table_name = '#{table}'
          AND iiconstraints.constraint_name = iicolumns.table_name
          AND iiconstraints.constraint_type = 'P'
          AND iicolumns.column_name != 'tidp'
          ORDER BY iicolumns.column_sequence
        end_sql

        row && row.first
      end

      def remove_index!(table_name, index_name)
        execute "DROP INDEX #{quote_table_name(index_name)}"
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

      def add_column(table_name, column_name, type, options = {})
        add_column_sql = "ALTER TABLE #{table_name} ADD #{column_name} #{type_to_sql(type)}"
        if( (type.to_s=="string") && (options[:limit]!=nil) ) then
          add_column_sql.gsub!("varchar(255)", "varchar(#{options[:limit]})")
          # puts ("\ntype.to_s.class is #{type.to_s.class}")
        end
        execute(add_column_sql)
      end

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

      private

      #def exec_no_cache(sql, binds)
      #  @connection.execute(sql)
      #end

      #def exec_cache(sql, binds)
      #  begin
      #    stmt_key = prepare_statement sql
      #  end
      #end

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
        exec_query(sql, name, binds)
      end

      def column_definitions(table_name)
        exec_query(<<-end_sql, 'SCHEMA')
          SELECT column_name, column_datatype, column_default_val
          FROM iicolumns
          WHERE table_name='#{table_name}'
          ORDER BY column_sequence
        end_sql
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
