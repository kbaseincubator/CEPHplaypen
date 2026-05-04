./warp mixed \
  --host=minio.berdl.kbase.us \
  --access-key=$S3_ACCESS_KEY \
  --secret-key=$S3_ACCESS_SECRET \
  --tls=true \
  --duration=120s \
  --obj.size=512KB \
  --concurrent=20 \
  --obj.randsize=false \
  --benchdata=minio-512k \
  --bucket warp-benchmark-test

