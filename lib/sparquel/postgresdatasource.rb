require 'sparquel/exception'
require 'pg'
require 'forwardable'

module Sparquel
  class PostgresDataSource
    def initialize(name:, host: 'localhost', port: 5432, database: 'postgres', user: ENV['LOGNAME'], password:)
      @name = name
      @host = host
      @port = port
      @database = database
      @user = user
      @password = password

      @connection = nil
    end

    attr_reader :name

    def parameters
      record = { name: @name, host: @host, port: @port, database: @database, user: @user }
      LiteralResult.new(self, [PostgresGenericRecord.new(record)])
    end

    def connection
      @connection ||= open
    end

    def open
      conn = PG::Connection.open(host: @host, port: @port, dbname: @database, user: @user, password: @password)
      PostgresConnection.new(self, conn)
    end

    def close!
      @connection.close if @connection
    rescue => err
      # ignore
      $stderr.puts err.message
    end

    extend Forwardable

    def_delegators 'connection',
      :execute,
      :list_schema,
      :current_schema,
      :current_schema=,
      :schema
  end

  class PostgresConnection
    def initialize(ds, connection)
      @ds = ds
      @connection = connection
      @current_schema_name = nil
    end

    def close
      @connection.close
    end

    def execute(sql, params = nil)
$stderr.puts "[SQL] #{sql}#{params ? ' ' + params.inspect : ''}"
      rs = params ? @connection.exec_params(sql, params) : @connection.exec(sql)
      PostgresResult.new(@ds, rs)
    rescue PG::Error => err
      raise SQLError, err.message
    end

    def session_parameter(name)
      # SHOW statement does not support parameters
      rec = execute("show #{name};").first or return nil
      rec.to_a.first
    end

    def search_path
      @search_path ||= begin
        if path = session_parameter('search_path')
          path.split(', ')
        else
          []
        end
      end
    end

    def sync_search_path_with_current_schema(name)
      path = search_path.dup
      path.unshift name unless path.first == name
      execute "set search_path = #{path.join(', ')};"
    end

    def prepend_search_path(name)
      self.search_path = [name] + search_path
    end

    def current_schema_name
      @current_schema_name ||= search_path.first
    end

    def list_schema
      q = 'select schema_name from information_schema.schemata order by 1;'
      execute(q).attach_record_class(PostgresSchema)
    end

    def current_schema
      schema(current_schema_name)
    end

    def current_schema=(schema)
      raise CommandOptionError, "no such schema: #{schema.name}" unless schema.exist?
      return if schema.name == @current_schema_name
      sync_search_path_with_current_schema schema.name
      @current_schema_name = schema.name
    end

    def schema(name)
      PostgresSchema.new(@ds, name)
    end
  end

  class PostgresSchema
    def PostgresSchema.for_record(rec, ds)
      new(ds, rec['schema_name'])
    end

    def initialize(ds, name)
      @ds = ds
      @name = name
    end

    attr_reader :name

    def inspect
      %Q(<\##{self.class} #{@name}>)
    end

    def exist?
      q = "select 1 from information_schema.schemata where schema_name = $1 order by 1;"
      @ds.execute(q, [@name]).to_a.size == 1
    end

    def objects
      q = "select table_schema, table_name, table_type from information_schema.tables where table_schema = $1 order by 1;"
      @ds.execute(q, [name]).attach_record_class(PostgresObject)
    end

    def matched_objects(pattern)
      q = "
          select table_schema, table_name, table_type
          from information_schema.tables
          where table_schema = $1
              and table_name #{match_operator} $2
          order by 1;
      "
      @ds.execute(q, [@name, compile_pattern(pattern)]).attach_record_class(PostgresObject)
    end

    private

    def match_operator
      '~'
    end

    # shell glob -> POSIX regex
    def compile_pattern(pat)
      '^' + pat.gsub('*', '.*').gsub('?', '.') + '$'
    end
  end

  class PostgresObject
    def PostgresObject.for_record(rec, ds)
      new(ds, rec['table_schema'], rec['table_name'], rec['table_type'].split.last.downcase)
    end

    def initialize(ds, schema, name, type)
      @ds = ds
      @schema = schema
      @name = name
      @type = type
    end

    attr_reader :schema
    attr_reader :name
    attr_reader :type

    def inspect
      %Q(<\##{self.class} #{@type} #{qualified_name}>)
    end

    def qualified_name
      "#{@schema}.#{@name}"
    end

    def drop
      @ds.execute "drop #{type} #{qualified_name} cascade;"
    end
  end

  class PostgresResult
    include Enumerable

    def initialize(ds, result)
      @ds = ds
      @result = result
      @record_class = nil
    end

    def attach_record_class(c)
      @record_class = c
      self
    end

    def each
      @result.each do |record|
        r = PostgresGenericRecord.for_record(record)
        yield @record_class ? @record_class.for_record(r, @ds) : r
      end
    end

    def display
      each do |record|
        p record
      end
    end
  end

  class PostgresGenericRecord
    class << self
      alias for_record new
    end

    def initialize(record)
      @record = record
    end

    def [](key)
      @record[key]
    end

    def to_a
      @record.values
    end

    def to_h
      @record.dup
    end
  end
end
