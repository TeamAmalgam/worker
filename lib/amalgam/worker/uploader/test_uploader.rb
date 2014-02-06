require 'fileutils'
class Amalgam::Worker::Uploader::TestUploader
  def initialize(options, old_uploader = nil)
    @destination_directory = options[:destination_directory]
  end

  def upload(src_path, key)
    dest_path = File.join(@destination_directory, key)
    FileUtils.mkdir_p(File.dirname(dest_path))
    FileUtils.copy_file(src_path, dest_path)
  end
end

Amalgam::Worker::Uploader.register_uploader(
    :test,
    Amalgam::Worker::Uploader::TestUploader
)