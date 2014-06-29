require 'sparquel/datasourcemanager'
require 'sparquel/evaluable'
require 'sparquel/builtins'
require 'sparquel/exception'
require 'readline'

module Sparquel
  class CommandLineEvaluator
    def CommandLineEvaluator.main
      prompt = Prompt.new('> ')
      input = $stdin.tty? ? TerminalInput.new(prompt) : FileInput.new($stdin)
      dsmgr = DataSourceManager.new
      new(input, dsmgr).mainloop
    end

    def initialize(input, data_source_manager)
      @reader = StatementReader.new(self, input)
      @data_source_manager = data_source_manager
    end

    def data_source
      @data_source_manager.current
    end

    def mainloop
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
      @data_source_manager.close
    end

    def exit
      throw :sparqual_main_loop
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

  class TerminalInput
    def initialize(prompt)
      @prompt = prompt
    end

    def readline
      Readline.readline(@prompt.get)
    end
  end

  class FileInput
    def initialize(f)
      @f = f
    end

    def readline
      @f.gets
    end
  end

  class StatementReader
    def initialize(evaluator, input)
      @evaluator = evaluator
      @input = input
    end

    def next
      while true
        line = @input.readline
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
end
