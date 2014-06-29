require 'sparquel/postgresdatasource'
require 'sparquel/evaluable'
require 'sparquel/builtins'
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
end
