module Sparquel
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
