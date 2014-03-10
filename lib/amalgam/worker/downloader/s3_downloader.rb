class Amalgam::Worker::Downloader::S3Downloader
  def initialize(options, old_downloader = nil)
    s3_client = AWS::S3.new
    @s3_bucket = s3_client.buckets[options[:s3_bucket]]
  end

  def download(key, destination_path)
    obj = @s3_bucket.objects[key]
    File.open(destination_path, "w") do |f|
      obj.read do |chunk|
        f.write(chunk)
      end
    end
  end
end

Amalgam::Worker::Downloader.register_downloader(
  :s3,
  Amalgam::Worker::Downloader::S3Downloader
)