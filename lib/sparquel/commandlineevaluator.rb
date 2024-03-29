require 'sparquel/applicationbuilder'
require 'sparquel/datasourcemanager'
require 'sparquel/evaluable'
require 'sparquel/builtins'
require 'sparquel/exception'
require 'readline'

module Sparquel
  class CommandLineEvaluator
    def CommandLineEvaluator.main
      evaluator = ApplicationBuilder.build
      evaluator.mainloop
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
      @data_source_manager.close!
    end

    def exit
      throw :sparqual_main_loop
    end
  end

  class Prompt
    def initialize(template)
      @template = template
    end

    attr_accessor :template
    attr_accessor :evaluator

    def get
      @template.gsub(/%./) {|mark|
        case mark
        when '%%' then '%'
        when '%s' then @evaluator.data_source.name
        when '%h' then @evaluator.data_source.host
        when '%p' then @evaluator.data_source.port
        when '%d' then @evaluator.data_source.database
        when '%u' then @evaluator.data_source.user
        when '%w' then @evaluator.data_source.current_schema.name
        else
          raise ConfigError, "unknown prompt variable: #{mark.inspect}"
        end
      }
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
