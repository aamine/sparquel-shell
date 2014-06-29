require 'sparquel/evaluable'
require 'sparquel/exception'

module Sparquel
  class ExitCommand < MetaCommand
    declare 'exit'
    declare 'quit'
    declare '\q'

    def execute
      evaluator.exit
      nil
    end
  end

  class ShowDataSourceCommand < MetaCommand
    declare 'conn'
    declare 'ds'

    def execute
      data_source.parameters
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
end
