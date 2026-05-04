./warp mixed \
  --host=minio.berdl.kbase.us \
  --access-key=$S3_ACCESS_KEY \
  --secret-key=$S3_ACCESS_SECRET \
  --tls=true \
  --duration=180s \
  --objects=50 \
  --obj.size=1GiB \
  --concurrent=4 \
  --obj.randsize=false \
  --benchdata=minio-1g \
  --bucket warp-benchmark-test

