module Sparquel
  class ApplicationError < StandardError; end
  class UserInputError < ApplicationError; end
  class CommandOptionError < UserInputError; end
  class SQLError < ApplicationError; end
end
