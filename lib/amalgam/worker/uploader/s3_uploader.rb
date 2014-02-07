class Amalgam::Worker::Uploader::S3Uploader
  def initialize(options, old_uploader)
    s3_client = AWS::S3.new
    @s3_bucket = s3_client.buckets[options[:s3_bucket]]
  end

  def upload(src_path, key)
    @s3_bucket.objects[key].write(:file => src_path)
  end
end

Amalgam::Worker::Uploader.register_uploader(
  :s3,
  Amalgam::Worker::Uploader::S3Uploader
)