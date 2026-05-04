./warp mixed \
  --host=minio.berdl.kbase.us \
  --access-key=$S3_ACCESS_KEY \
  --secret-key=$S3_ACCESS_SECRET \
  --tls=true \
  --duration=120s \
  --obj.size=4KB \
  --concurrent=50 \
  --obj.randsize=false \
  --benchdata=minio-4k \
  --bucket warp-benchmark-test

