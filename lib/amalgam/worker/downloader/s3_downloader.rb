class Amalgam::Worker::Downloader::S3Downloader
  def initialize(options, old_downloader = nil)
    @s3_bucket = options[:s3_bucket]
  end

  def download(key, destination_path)
    raise "Not implemented yet."
  end
end

Amalgam::Worker::Downloader.register_downloader(
  :s3,
  Amalgam::Worker::Downloader::S3Downloader
)