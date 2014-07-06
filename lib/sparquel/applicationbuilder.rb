require 'yaml'

module Sparquel
  class ApplicationBuilder
    def ApplicationBuilder.build
      b = new
      b.load_defaults
      b.build
    end

    def load_defaults
      load File.expand_path('~/.sparquel/config')
    end

    def load(path)
      @config = YAML.load(File.read(path))
    end

    def config(key)
      @config.fetch(key)
    end

    def build
      evaluator = CommandLineEvaluator.new(input, data_source_manager)
      prompt.evaluator = evaluator
      evaluator
    end

    def prompt
      @prompt ||= Prompt.new(config('prompt'))
    end

    def input
      @input ||= $stdin.tty? ? TerminalInput.new(prompt) : FileInput.new($stdin)
    end

    def data_source_manager
      @data_source_manager ||= DataSourceManager.new
    end
  end
end
