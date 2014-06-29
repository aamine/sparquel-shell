Gem::Specification.new do |s|
  s.platform = Gem::Platform::RUBY
  s.name = 'sparquel'
  s.version = '0.1.0'
  s.summary = 'Sparquel SQL shell'
  s.description = 'powerful SQL shell'
  s.license = 'MIT'

  s.author = ['Minero Aoki']
  s.email = 'mineroaoki@gmail.com'
  s.homepage = 'http://github.com/aamine/sparquel'

  s.files = Dir.glob(['LICENSE', 'README.md', 'lib/**/*.rb'])
  s.require_path = 'lib'

  s.required_ruby_version = '>= 2.0.0'
  s.add_dependency 'pg'
  s.add_dependency 'racc'
  s.add_dependency 'pry'
end
