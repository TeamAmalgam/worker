class Amalgam::Worker::Uploader::S3Uploader
  def initialize(options, old_uploader)
    @s3_bucket = options[:s3_bucket]
  end

  def upload(src_path, key)
    raise "Not implemented yet."
  end
end

Amalgam::Worker::Uploader.register_uploader(
  :s3,
  Amalgam::Worker::Uploader::S3Uploader
)