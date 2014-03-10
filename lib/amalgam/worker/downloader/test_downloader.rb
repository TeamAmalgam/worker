require 'fileutils'
class Amalgam::Worker::Downloader::TestDownloader
  def initialize(options, old_downloader)
    @source_directory = options[:source_directory]
  end

  def download(key, destination_path)
    source_path = File.join(@source_directory, key)
    FileUtils.copy_file(source_path, destination_path)
  end
end

Amalgam::Worker::Downloader.register_downloader(
  :test,
  Amalgam::Worker::Downloader::TestDownloader
)