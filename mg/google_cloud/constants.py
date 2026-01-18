GCP_PROJECT = "dfs-sim"
DFS_SIM_CREDS = "dfs_sim_service_account.json"

# Personal Buckets
DATA_BUCKET = "data-bucket-dfs-sim"

# Credentials to Bucket Mapping
BUCKET_CREDS = {
    DATA_BUCKET: DFS_SIM_CREDS,
}

# Environment Variables
ENV_CREDS_PATH = {
    "dfs_sim_service_account.json": "DFS_SIM_CREDS",
}

# SPORT BUCKETS
SPORT_BUCKET = {"cfb": "cfb-storage-bucket"}
