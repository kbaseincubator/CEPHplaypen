./warp mixed \
  --host=minio.berdl.kbase.us \
  --access-key=$S3_ACCESS_KEY \
  --secret-key=$S3_ACCESS_SECRET \
  --tls=true \
  --objects=200 \
  --duration=120s \
  --obj.size=50MB \
  --concurrent=10 \
  --obj.randsize=false \
  --benchdata=minio-50m \
  --bucket warp-benchmark-test

