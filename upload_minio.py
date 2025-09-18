import os, glob, fsspec
fs = fsspec.filesystem(
    "s3",
    key=os.environ["S3_ACCESS_KEY"],
    secret=os.environ["S3_SECRET_KEY"],
    client_kwargs={"endpoint_url":os.environ["S3_ENDPOINT"], "region_name":os.environ.get("S3_REGION","us-east-1")},
    config_kwargs={"s3":{"addressing_style":"path"}},
)

# Make bucket and prefix
try: fs.mkdir("s3://mlstore")
except Exception as e: print("mkdir:", e)
try: fs.mkdirs("s3://mlstore/dataset")
except Exception as e: print("mkdirs:", e)

# Upload local .\dataset\ files
for p in glob.glob(r"dataset\*"):
    key = f"s3://mlstore/dataset/{os.path.basename(p)}"
    with open(p, "rb") as fsrc, fs.open(key, "wb") as fdst:
        fdst.write(fsrc.read())
    print("uploaded:", key)

print("Listing sample:", fs.ls("s3://mlstore/dataset")[:5])
