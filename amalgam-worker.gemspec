Gem::Specification.new do |s|
  s.name        = 'amalgam-worker'
  s.version     = '0.1.0'
  s.date        = '2014-02-04'
  s.summary     = 'Worker program for Team Amalgam'
  s.description = 'Runs moolloy build jobs and test jobs'
  s.authors     = [ 'Chris Kleynhans' ]
  s.email       = 'chris@kleynhans.ca'
  s.homepage    = 'http://github.com/TeamAmalgam/worker'
  s.files       = ['lib/amalgam.rb',
                   'lib/amalgam/worker.rb',
                   'lib/amalgam/worker/manager.rb',
                   'lib/amalgam/worker/runner.rb',
                   'lib/amalgam/worker/job.rb',
                   'lib/amalgam/worker/jobs/build_job.rb',
                   'lib/amalgam/worker/jobs/run_job.rb']
  s.executables = ['amalgam-worker']

  s.add_runtime_dependency 'aws-sdk', '~> 1.33.0'
  s.add_runtime_dependency 'safe_yaml', '~> 1.0.1'
  s.add_runtime_dependency 'rugged', '~> 0.19.0'
  s.add_runtime_dependency 'httparty', '~> 0.12.0'

  s.add_development_dependency 'rake', '~> 10.1.1'
  s.add_development_dependency 'rspec', '~> 2.14.1'
end
