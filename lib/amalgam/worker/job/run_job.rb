require 'benchmark'
require 'fileutils'
require 'digest'

class Amalgam::Worker::Job::RunJob < Amalgam::Worker::Job
    def initialize(job_description, configuration)
      Amalgam::Worker.logger.info("Creating RunJob")
      validate_job(job_description)

      @job_description = job_description
      @configuration = configuration
    end

    def run
      run_main
    end

    def terminate
      # Need to implement
    end

    private

    REQUIRED_PARAMETERS = [
      :jar_file_key,
      :model_file_key
    ]

    def validate_job(job_description)
      REQUIRED_PARAMETERS.each do |param|
        if job_description[param].nil?
          raise "Job missing required parametter: #{param}"
        end
      end
    end

    def sha2_hash(filename)
      digest = Digest::SHA2.new

      File.open(filename) do |file|
        while not file.eof
          digest << file.read(digest.block_length)
        end
      end

      digest.digest
    end

    def run_main
      Amalgam::Worker.logger.info("Running RunJob")
      working_directory = Dir.getwd
      model_directory = File.join(working_directory, "model")
      run_directory = File.join(working_directory, "run")
      package_directory = File.join(working_directory, "package")

      Dir.mkdir(model_directory)
      Dir.mkdir(run_directory)
      Dir.mkdir(package_directory)

      begin
        Dir.chdir(model_directory)

        # Download the model into the model directory.

        model_tar_file_path = File.join(model_directory,
                                        "model.tar.bz2")
        downloader = @configuration.downloader
        downloader.download(@job_description[:model_file_key], model_tar_file_path)

        # Unpack the model.

        tar_result = `tar -xf #{model_tar_file_path}`
        if $? != 0
          return {
            :return_code => $?,
            :error_message => "Failed to unpack the model.",
            :error_detail => tar_result
          }
        end

        model_als_path = File.join(model_directory, "model.als")

        Dir.chdir(run_directory)

        # Download the moolloy jar.

        jar_file_path = File.join(run_directory,
                                  "moolloy.jar")

        downloader = @configuration.downloader
        downloader.download(@job_description[:jar_file_key], jar_file_path)

        # Run moolloy.
        stdout_path = File.join(run_directory, "stdout.out")
        stderr_path = File.join(run_directory, "stderr.out")

        benchmark_result = Benchmark.measure do
          `java -jar "#{jar_file_path}" "#{model_als_path}" > #{stdout_path} 2> #{stderr_path}`
        end

        return_code = $?

        # Determine correctness.

        correct = (return_code == 0)
        test_solution_files = Dir[File.join(run_directory,
                                            "alloy_solutions_*.xml")]
        model_solution_files = Dir[File.join(model_directory,
                                             "alloy_solutions_*.xml")]

        if test_solution_files.count != model_solution_files.count
          correct = false
        end

        hash_to_model_solution = {}
        model_solution_files.each do |model_file|
          `tail -n +2 #{model_file} > #{model_file}.trimmed`
          hash = sha2_hash(model_file + ".trimmed")
          hash_to_model_solution[hash] ||= []
          hash_to_model_solution[hash] << model_file
        end

        test_solution_files.each do |test_file|
          `tail -n +2 #{test_file} > #{test_file}.trimmed`
          hash = sha2_hash(test_file + ".trimmed")

          if !hash_to_model_solution[hash].nil? &&
             !hash_to_model_solution[hash].empty?

            matching_file = hash_to_model_solution[hash].shift
            Amalgam::Worker.logger.info("#{test_file} matches #{matching_file}.")
          else
            Amalgam::Worker.logger.info("#{test_file} has no match.")
            correct = false
          end
        end

        if correct
          Amalgam::Worker.logger.info("The solutions are correct.")
        else
          Amalgam::Worker.logger.info("The solutions are incorrect.")
        end

        # Package results
        FileUtils.mv(model_directory, package_directory)
        FileUtils.mv(run_directory, package_directory)

        tarball_path = File.join(working_directory, "tarball.tar.bz2")

        Dir.chdir(package_directory)

        `hostname > ./hostname`
        tar_result = `tar -cjf "#{tarball_path}" #{File.join(".", "*")}`
        if $? != 0
          return {
            :return_code => $?,
            :error_message => "Failed to package results.",
            :error_detail => tar_result
          }
        end 

        # Upload the package to s3
        key = "results/#{@job_description[:job_id]}.tar.bz2"
        uploader = @configuration.uploader
        uploader.upload(tarball_path, key)

        return {
          :return_code => return_code,
          :correct => (correct ? 1 : 0),
          :runtime_seconds => benchmark_result.real,
          :cpu_time_seconds => benchmark_result.total,
          :tarball_s3_key => key
        }
      ensure
        Dir.chdir(working_directory)
      end
    end
end

Amalgam::Worker::Job.register_job(:run, Amalgam::Worker::Job::RunJob)
