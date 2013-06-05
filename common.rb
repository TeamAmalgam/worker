def sha2_hash(filename)
  digest = Digest::SHA2.new

  File.open(filename) do |file|
    while not file.eof
      digest << file.read(digest.block_length)
    end
  end

  digest.digest
end
