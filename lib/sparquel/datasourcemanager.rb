require 'sparquel/exception'
require 'yaml'

module Sparquel
  class DataSourceManager
    def initialize
      @data_sources = {}
      load_default
      @current = default()
    end

    attr_reader :current

    def load_default
      path = File.expand_path('~/.sparquel/datasource.yml')
      load path if File.exist?(path)
    end

    def load(path)
      YAML.load(File.read(path)).each do |name, _params|
        _params['name'] = name
        params = {}
        _params.each do |name, value|
          params[name.intern] = value
        end
        @data_sources[name] = new_data_source(name, params)
      end
    rescue IOError, Psych::Exception => err
      raise ConfigError, err.message
    end

    def new_data_source(name, params)
      c = params.delete(:class) or raise ConfigError, "missing data source class: #{name}"
      class_name = "#{c.capitalize}DataSource"
      require "sparquel/#{class_name.downcase}"
      ::Sparquel.const_get(class_name).new(**params)
    rescue => err
      raise ConfigError, "could not create data source: #{name}: #{err.message}"
    end

    def default
      @data_sources.values.first or raise ConfigError, "no data source"
    end

    def [](name)
      @data_sources[name] or raise ConfigError, "no such data source: #{name}"
    end
  end
end
