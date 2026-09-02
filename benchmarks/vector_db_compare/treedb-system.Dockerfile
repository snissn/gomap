FROM scratch
COPY treedb_vector_partition_bench /treedb_vector_partition_bench
ENTRYPOINT ["/treedb_vector_partition_bench"]
