FROM us-central1-docker.pkg.dev/rapids-spark/rapids-serverless/rapids2608-gcs-range@sha256:0b0e39b84a7b684176fed917a604547f298e30876e060b58458d6fe89127ad23

LABEL org.opencontainers.image.revision="d0e01011aa3517147098d12c68d2a181ef3d15d6"
LABEL com.nvidia.rapids.jar.sha256="5bfa48cd137e5cf497b581d475fe6371d523f6bb0d79c6079d7a9c78d899c6db"
LABEL com.nvidia.rapids.spark.classifier="spark351"
LABEL com.nvidia.rapids.scala.binary.version="2.13"

COPY --chown=spark:spark rapids-4-spark_2.13-26.08.0-SNAPSHOT-cuda12.jar \
  /opt/spark/jars/rapids-4-spark_2.13-26.08.0-SNAPSHOT-hostalloc-extendeddiag-spark351-cuda12.jar
