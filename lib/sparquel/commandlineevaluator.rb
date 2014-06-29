require 'sparquel/postgresdatasource'
require 'sparquel/exception'
require 'readline'
require 'forwardable'

module Sparquel
  class CommandLineEvaluator
    def CommandLineEvaluator.main
      new.mainloop
    end

    def initialize
      @prompt = Prompt.new('> ')
      @data_source = PostgresDataSource.new(name: 'dev', host: 'localhost', port: 5432, database: 'dev', user: 'aamine', password: nil)
    end

    attr_reader :prompt
    attr_reader :data_source

    def mainloop
      @reader = StatementReader.new(self, @prompt)
      catch(:sparqual_main_loop) {
        while true
          begin
            stmt = @reader.next or break
            result = stmt.execute
            result.display if result
          rescue ApplicationError => err
            $stderr.puts err.message
          end
        end
      }
      close_all_data_sources
    end

    def exit
      throw :sparqual_main_loop
    end

    def close_all_data_sources
      @data_source.close!
    end
  end

  class StatementReader
    def initialize(evaluator, prompt)
      @evaluator = evaluator
      @prompt = prompt
    end

    def next
      while true
        line = Readline.readline(@prompt.get)
        stmt = if line.strip.empty?
          nil
        elsif meta_command?(line)
          read_meta_command(line)
        else
          read_sql_statement(line)
        end
        next unless stmt
        return stmt
      end
    rescue Interrupt
      $stderr.puts "interrupted"
      retry
    rescue IOError => err
      $stderr.puts err.message if $VERBOSE
      nil
    end

    def meta_command?(first_line)
      MetaCommand.declared?(first_line.strip.split.first)
    end

    def read_meta_command(first_line)
      cmd, *args = first_line.strip.split
      MetaCommand.polymorphic_new(@evaluator, cmd, args)
    end

    def read_sql_statement(first_line)
      SQLStatement.new(@evaluator, first_line.strip)
    end
  end

  class Prompt
    def initialize(template)
      @template = template
    end

    def get
      @template.dup
    end
  end

  class Evaluable
    def initialize(evaluator)
      @evaluator = evaluator
    end

    private

    attr_reader :evaluator

    def data_source
      @evaluator.data_source
    end

    def execute_query(sql)
      @evaluator.data_source.connection.execute(sql)
    end

    extend Forwardable

    def_delegators '@evaluator.data_source',
      :list_schema,
      :current_schema,
      :schema
  end

  class SQLStatement < Evaluable
    def initialize(evaluator, source)
      super evaluator
      @source = source
    end

    attr_reader :source

    def execute
      execute_query(@source)
    end
  end

  class MetaCommand < Evaluable
    COMMANDS = {}

    def self.declare(name)
      COMMANDS[name] = self
    end

    def MetaCommand.names
      COMMANDS.keys
    end

    def MetaCommand.declared?(name)
      COMMANDS.key?(name)
    end

    def MetaCommand.polymorphic_new(evaluator, cmd, args)
      c = COMMANDS[cmd] or
          raise UserInputError, "unknown command: #{cmd}"
      c.new(evaluator, cmd, args)
    end

    def initialize(evaluator, cmd, args)
      super evaluator
      @cmd = cmd
      @args = args
    end

    attr_reader :cmd
    attr_reader :args

    def execute
      nil
    end
  end

  class ExitCommand < MetaCommand
    declare 'exit'
    declare 'quit'
    declare '\q'

    def execute
      evaluator.exit
      nil
    end
  end

  class CurrentSchemaCommand < MetaCommand
    declare 'pwd'

    def execute
      LiteralResult.new(data_source, [current_schema])
    end
  end

  class ChangeCurrentSchemaCommand < MetaCommand
    declare 'cd'

    def execute
      raise CommandOptionError, "missing schema name or too many names" unless @args.size == 1
      data_source.current_schema = schema(@args.first)
      nil
    end
  end

  class ListSchemaCommand < MetaCommand
    declare 'lsn'
    declare 'dn'
    declare '\dn'

    def execute
      list_schema
    end
  end

  class ListCommand < MetaCommand
    declare 'ls'

    def execute
      schemata = @args.empty? ? [current_schema] : @args.map {|s| schema(s) }
      CompositeResult.new(schemata.map(&:objects))
    end
  end

  class RemoveCommand < MetaCommand
    declare 'rm'

    def execute
      @args.each do |pattern|
        current_schema.matched_objects(pattern).each do |obj|
          obj.drop
        end
      end
      nil
    end
  end

  class CompositeResult
    def initialize(results)
      @results = results
    end

    def each(&block)
      @results.each do |result|
        result.each(&block)
      end
    end

    def to_a
      @results.map {|r| r.to_a }.flatten
    end

    def display
      @results.each do |result|
        result.display
      end
    end
  end

  class LiteralResult
    include Enumerable

    def initialize(ds, records)
      @ds = ds
      @records = records
    end

    def each(&block)
      @records.each(&block)
    end

    def display
      each do |record|
        p record
      end
    end
  end
end
