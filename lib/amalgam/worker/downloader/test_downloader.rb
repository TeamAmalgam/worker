require 'fileutils'
class Amalgam::Worker::Downloader::TestDownloader
  def initialize(file_map, old_downloader)
    @file_map = file_map || {}
  end

  def download(key, destination_path)
    raise "No such key." if @file_map[key].nil? 
    
    source_path = @file_map[key]
    FileUtils.copy_file(source_path, destination_path)
  end
end

Amalgam::Worker::Downloader.register_downloader(
  :test,
  Amalgam::Worker::Downloader::TestDownloader
)