import time
import zarr
import gcsfs
import fsspec
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.implementations.cached import SimpleCacheFileSystem, WholeFileCacheFileSystem
from asyncachefs import AsyncCachingFileSystem

import shutil
import os


# if os.path.exists("cache"):
#     shutil.rmtree("cache")

print(f"Zarr version: {zarr.__version__}")
cache_options = {"cache_storage": "cache"}
fs = gcsfs.GCSFileSystem(cache_timeout=-1, token="anon",access="read_only", asynchronous=True)
fs = AsyncCachingFileSystem(fs=fs, **cache_options, asynchronous=True)

print(fs._cat_file)
# fs = AsyncFileSystemWrapper(fs)

if (zarr.__version__ < "3" and zarr.__version__ > "2"):
    zstore = fsspec.FSMap(
        "gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3", fs
    )
else:
    zstore = zarr.storage.FsspecStore(
        fs,
        path="/gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3",
    )
zarr_group = zarr.open(store=zstore, mode="r")


start = time.time()
level_coords = zarr_group["100m_u_component_of_wind"][1078200:1078310,:]
end = time.time()
print(f"First read took: {end - start:.4f} seconds")

start = time.time() 
level_coords = zarr_group["100m_u_component_of_wind"][1078200:1078310,:]
end = time.time()
print(f"Second read took: {end - start:.4f} seconds")
