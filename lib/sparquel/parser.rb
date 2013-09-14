require 'racc'
require 'racc/grammar'
require 'strscan'
require 'stringio'

module Sparquel
  class Node
    def Node.define(*components)
      node_class = Class.new(self)
      node_class.module_eval(<<-End, __FILE__, __LINE__)
        def initialize(#{components.join(', ')})
          #{ components.map {|c| "@#{c} = #{c}" }.join('; ') }
        end

        def components
          [#{ components.map {|c| "@#{c}" }.join(', ') }]
        end

        def inspect
          "(\#{self.class.name.split('::').last} #{components.map {|c| "#{c}=\#{@#{c}.inspect}" }.join(', ')})"
        end
      End
      components.each do |c|
        node_class.send :attr_reader, c
      end
      node_class.instance_variable_set :@_components, components
      class << node_class
        attr_reader :_components
      end
      node_class
    end
  end

  SelectStatement = Node.define(:exprs, :from, :options)
  Relation = Node.define(:ref)
  Subquery = Node.define(:name, :select)
  Literal = Node.define(:value)
  Reference = Node.define(:ref)
  Join = Node.define(:type, :cond, :left, :right)
  CondExpr = Node.define(:op, :left, :right)

  grammar = Racc::Grammar.define {
    g = self

    g.statement = seq(:select, :term) {|select, _|
                    select
                  }

    g.select    = seq(:SELECT, option(:top), :exprs, option(:from_expr)) {|_, top, exprs, from|
                    SelectStatement.new(exprs, from, top: top)
                  }

    g.top       = seq(:TOP, :NUMBER) {|_, num| num }

    g.from_expr = seq(:FROM, :relations) {|_, rels| rels }

    g.relations = seq(:relation)\
                | seq(:relations, :join_type, :relation, :ON, :cond_expr) {|l, type, r, _, cond| Join.new(type, cond, l, r) }

    g.cond_expr = seq(:expr, :CMP_OP, :expr) {|l, op, r| CondExpr.new(op, l, r) }

    g.relation  = seq(:IDENT) {|ref|
                    Relation.new(ref)
                  }\
                | seq(:subquery)\
                | seq('(', :relations, ')') {|_, rels, _| rels }

    g.join_type = seq(option(:INNER), :JOIN) {|*| :inner }\
                | seq(option(:FULL), :OUTER, :JOIN) {|*| :full_outer }\
                | seq(:LEFT, option(:OUTER), :JOIN) {|*| :left_outer }\
                | seq(:RIGHT, option(:OUTER), :JOIN) {|*| :right_outer }

    g.subquery  = seq('(', :select, ')', option(:IDENT)) {|_, select, _, name|
                    Subquery.new(name, select)
                  }

    g.exprs     = separated_by1(',', :expr)

    g.expr      = seq(:NUMBER) {|num|
                    Literal.new(num)
                  }\
                | seq(:IDENT) {|ident|
                    Reference.new(ident)
                  }

    g.term      = seq(';') | seq(:EOF)
  }
  Parser = grammar.parser_class

  class Parser   # reopen
    def Parser.parse_stream(stream, name)
      new(SourceInput.new(stream, name)).parse
    end

    def Parser.parse_file(path)
      File.open(path) {|f| new(SourceInput.new(f, path)) }
    end

    def Parser.parse_string(src)
      parse_stream(StringIO.new(src), '-')
    end

    def initialize(source)
      @source = source
    end

    def parse
      @yydebug = $DEBUG
      yyparse(self, :yylex)
    end

    private

    RESERVED_WORDS = {
      'select' => :SELECT,
      'from' => :FROM,
      'where' => :WHERE,
      'group' => :GROUP,
      'order' => :ORDER,
      'by' => :BY,
      'top' => :TOP,
      'limit' => :LIMIT,
      'sample' => :SAMPLE,
      'qualify' => :QUALIFY,
      'over' => :OVER,
      'partition' => :PARTITION,
      'case' => :CASE,
      'when' => :WHEN,
      'then' => :THEN,
      'inner' => :INNER,
      'outer' => :OUTER,
      'left' => :LEFT,
      'right' => :RIGHT,
      'full' => :FULL,
      'join' => :JOIN,
      'on' => :ON,
      'true' => :TRUE,
      'false' => :FALSE,
      'and' => :AND,
      'or' => :OR,
      'is' => :IS,
      'null' => :NULL,
      'end' => :END
    }

    def yylex
      s = StringScanner.new('')
      @source.each_line do |line|
        s << line
        while true
          case
          when s.eos?
            yield [:EOF, nil]
            break
          when s.skip(/\s+/)
            ;
          when tok = s.scan(/\d+/)
            yield [:NUMBER, tok.to_i]
          when tok = s.scan(/\w+/i)
            if sym = RESERVED_WORDS[tok.downcase]
              yield [sym, nil]
            else
              yield [:IDENT, tok]
            end
          when tok = s.scan(%r{=|!=|<>|>|>=|<|<=})
            yield [:CMP_OP, tok]
          when tok = s.scan(%r{;})
            yield [tok, tok]
          else
            c = s.getch
            yield [c, c]
          end
        end
      end
      yield nil
    end
  end

  class SourceInput
    def initialize(f, name)
      @f = f
      @name = name
    end

    attr_reader :name

    def each_line(&block)
      @f.each(&block)
    end
  end
end

if $0 == __FILE__
  p Sparquel::Parser.parse_string(ARGV[0])
end
