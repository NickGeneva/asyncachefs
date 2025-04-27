from __future__ import annotations

from hashlib import sha256
import inspect
import logging
import os
import tempfile
import time
import weakref
from shutil import rmtree
from typing import TYPE_CHECKING, Any, Callable, ClassVar

from fsspec.asyn import AsyncFileSystem
from fsspec import AbstractFileSystem, filesystem
from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.compression import compr
from fsspec.core import BaseCache, MMapCache
from fsspec.exceptions import BlocksizeMismatchError
from fsspec.implementations.cache_mapper import create_cache_mapper
from fsspec.implementations.cache_metadata import CacheMetadata
from fsspec.spec import AbstractBufferedFile
from fsspec.transaction import Transaction
from fsspec.utils import infer_compression, isfilelike


if TYPE_CHECKING:
    from fsspec.implementations.cache_mapper import AbstractCacheMapper

logger = logging.getLogger("fsspec.cached")


class WriteCachedTransaction(Transaction):
    def complete(self, commit=True):
        rpaths = [f.path for f in self.files]
        lpaths = [f.fn for f in self.files]
        if commit:
            self.fs.put(lpaths, rpaths)
        self.files.clear()
        self.fs._intrans = False
        self.fs._transaction = None
        self.fs = None  # break cycle


# https://github.com/NickGeneva/filesystem_spec/blob/ngeneva/cache_async/fsspec/asyn.py

async_methods = [
    "_ls",
    "_cat_file",
    "_get_file",
    "_put_file",
    "_rm_file",
    "_cp_file",
    "_pipe_file",
    "_expand_path",
    "_info",
    "_isfile",
    "_isdir",
    "_exists",
    "_walk",
    "_glob",
    "_find",
    "_du",
    "_size",
    "_mkdir",
    "_makedirs",
]


class AsyncCachingFileSystem(AsyncFileSystem):
    """Async locally caching filesystem, layer over any other FS

    This class implements chunk-wise local storage of remote files, for quick
    access after the initial download. The files are stored in a given
    directory with hashes of URLs for the filenames. If no directory is given,
    a temporary one is used, which should be cleaned up by the OS after the
    process ends. The files themselves are sparse (as implemented in
    :class:`~fsspec.caching.MMapCache`), so only the data which is accessed
    takes up space.

    Restrictions:

    - the block-size must be the same for each access of a given file, unless
      all blocks of the file have already been read
    - caching can only be applied to file-systems which produce files
      derived from fsspec.spec.AbstractBufferedFile ; LocalFileSystem is also
      allowed, for testing
    """

    protocol: ClassVar[str | tuple[str, ...]] = ("blockcache", "cached")

    def __init__(
        self,
        target_protocol=None,
        cache_storage="TMP",
        cache_check=10,
        check_files=False,
        expiry_time=604800,
        target_options=None,
        fs=None,
        same_names: bool | None = None,
        compression=None,
        cache_mapper: AbstractCacheMapper | None = None,
        **kwargs,
    ):
        """

        Parameters
        ----------
        target_protocol: str (optional)
            Target filesystem protocol. Provide either this or ``fs``.
        cache_storage: str or list(str)
            Location to store files. If "TMP", this is a temporary directory,
            and will be cleaned up by the OS when this process ends (or later).
            If a list, each location will be tried in the order given, but
            only the last will be considered writable.
        cache_check: int
            Number of seconds between reload of cache metadata
        check_files: bool
            Whether to explicitly see if the UID of the remote file matches
            the stored one before using. Warning: some file systems such as
            HTTP cannot reliably give a unique hash of the contents of some
            path, so be sure to set this option to False.
        expiry_time: int
            The time in seconds after which a local copy is considered useless.
            Set to falsy to prevent expiry. The default is equivalent to one
            week.
        target_options: dict or None
            Passed to the instantiation of the FS, if fs is None.
        fs: filesystem instance
            The target filesystem to run against. Provide this or ``protocol``.
        same_names: bool (optional)
            By default, target URLs are hashed using a ``HashCacheMapper`` so
            that files from different backends with the same basename do not
            conflict. If this argument is ``true``, a ``BasenameCacheMapper``
            is used instead. Other cache mapper options are available by using
            the ``cache_mapper`` keyword argument. Only one of this and
            ``cache_mapper`` should be specified.
        compression: str (optional)
            To decompress on download. Can be 'infer' (guess from the URL name),
            one of the entries in ``fsspec.compression.compr``, or None for no
            decompression.
        cache_mapper: AbstractCacheMapper (optional)
            The object use to map from original filenames to cached filenames.
            Only one of this and ``same_names`` should be specified.
        """
        super().__init__(**kwargs)
        if fs is None and target_protocol is None:
            raise ValueError(
                "Please provide filesystem instance(fs) or target_protocol"
            )
        if not (fs is None) ^ (target_protocol is None):
            raise ValueError(
                "Both filesystems (fs) and target_protocol may not be both given."
            )
        if cache_storage == "TMP":
            tempdir = tempfile.mkdtemp()
            storage = [tempdir]
            weakref.finalize(self, self._remove_tempdir, tempdir)
        else:
            if isinstance(cache_storage, str):
                storage = [cache_storage]
            else:
                storage = cache_storage
        os.makedirs(storage[-1], exist_ok=True)
        self.storage = storage
        self.kwargs = target_options or {}
        self.cache_check = cache_check
        self.check_files = check_files
        self.expiry = expiry_time
        self.compression = compression

        # Size of cache in bytes. If None then the size is unknown and will be
        # recalculated the next time cache_size() is called. On writes to the
        # cache this is reset to None.
        self._cache_size = None

        if same_names is not None and cache_mapper is not None:
            raise ValueError(
                "Cannot specify both same_names and cache_mapper in "
                "CachingFileSystem.__init__"
            )
        if cache_mapper is not None:
            self._mapper = cache_mapper
        else:
            self._mapper = create_cache_mapper(
                same_names if same_names is not None else False
            )

        self.target_protocol = (
            target_protocol
            if isinstance(target_protocol, str)
            else (fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0])
        )
        self._metadata = CacheMetadata(self.storage)
        self.load_cache()
        self.fs = fs if fs is not None else filesystem(target_protocol, **self.kwargs)
        if not self.fs.async_impl:
            raise ValueError("Underlying filesystem needs to be async")


        def _strip_protocol(path):
            # acts as a method, since each instance has a difference target
            return self.fs._strip_protocol(type(self)._strip_protocol(path))

        self._strip_protocol: Callable = _strip_protocol

    @staticmethod
    def _remove_tempdir(tempdir):
        try:
            rmtree(tempdir)
        except Exception:
            pass

    def _mkcache(self):
        os.makedirs(self.storage[-1], exist_ok=True)

    def cache_size(self):
        """Return size of cache in bytes.

        If more than one cache directory is in use, only the size of the last
        one (the writable cache directory) is returned.
        """
        if self._cache_size is None:
            cache_dir = self.storage[-1]
            self._cache_size = filesystem("file").du(cache_dir, withdirs=True)
        return self._cache_size

    def load_cache(self):
        """Read set of stored blocks from file"""
        self._metadata.load()
        self._mkcache()
        self.last_cache = time.time()

    def save_cache(self):
        """Save set of stored blocks from file"""
        self._mkcache()
        self._metadata.save()
        self.last_cache = time.time()
        self._cache_size = None

    def _check_cache(self):
        """Reload caches if time elapsed or any disappeared"""
        self._mkcache()
        if not self.cache_check:
            # explicitly told not to bother checking
            return
        timecond = time.time() - self.last_cache > self.cache_check
        existcond = all(os.path.exists(storage) for storage in self.storage)
        if timecond or not existcond:
            self.load_cache()

    def _check_file(self, path):
        """Is path in cache and still valid"""
        path = self._strip_protocol(path)
        self._check_cache()
        return self._metadata.check_file(path, self)

    def clear_cache(self):
        """Remove all files and metadata from the cache

        In the case of multiple cache locations, this clears only the last one,
        which is assumed to be the read/write one.
        """
        rmtree(self.storage[-1])
        self.load_cache()
        self._cache_size = None

    def clear_expired_cache(self, expiry_time=None):
        """Remove all expired files and metadata from the cache

        In the case of multiple cache locations, this clears only the last one,
        which is assumed to be the read/write one.

        Parameters
        ----------
        expiry_time: int
            The time in seconds after which a local copy is considered useless.
            If not defined the default is equivalent to the attribute from the
            file caching instantiation.
        """

        if not expiry_time:
            expiry_time = self.expiry

        self._check_cache()

        expired_files, writable_cache_empty = self._metadata.clear_expired(expiry_time)
        for fn in expired_files:
            if os.path.exists(fn):
                os.remove(fn)

        if writable_cache_empty:
            rmtree(self.storage[-1])
            self.load_cache()

        self._cache_size = None

    def pop_from_cache(self, path):
        """Remove cached version of given file

        Deletes local copy of the given (remote) path. If it is found in a cache
        location which is not the last, it is assumed to be read-only, and
        raises PermissionError
        """
        path = self._strip_protocol(path)
        fn = self._metadata.pop_file(path)
        if fn is not None:
            os.remove(fn)
        self._cache_size = None

    # TODO:
    # def _open(
    #     self,
    #     path,
    #     mode="rb",
    #     block_size=None,
    #     autocommit=True,
    #     cache_options=None,
    #     **kwargs,
    # ):
    #     """Wrap the target _open

    #     If the whole file exists in the cache, just open it locally and
    #     return that.

    #     Otherwise, open the file on the target FS, and make it have a mmap
    #     cache pointing to the location which we determine, in our cache.
    #     The ``blocks`` instance is shared, so as the mmap cache instance
    #     updates, so does the entry in our ``cached_files`` attribute.
    #     We monkey-patch this file, so that when it closes, we call
    #     ``close_and_update`` to save the state of the blocks.
    #     """
    #     path = self._strip_protocol(path)

    #     path = self.fs._strip_protocol(path)
    #     if "r" not in mode:
    #         return self.fs._open(
    #             path,
    #             mode=mode,
    #             block_size=block_size,
    #             autocommit=autocommit,
    #             cache_options=cache_options,
    #             **kwargs,
    #         )
    #     detail = self._check_file(path)
    #     if detail:
    #         # file is in cache
    #         detail, fn = detail
    #         hash, blocks = detail["fn"], detail["blocks"]
    #         if blocks is True:
    #             # stored file is complete
    #             logger.debug("Opening local copy of %s", path)
    #             return open(fn, mode)
    #         # TODO: action where partial file exists in read-only cache
    #         logger.debug("Opening partially cached copy of %s", path)
    #     else:
    #         hash = self._mapper(path)
    #         fn = os.path.join(self.storage[-1], hash)
    #         blocks = set()
    #         detail = {
    #             "original": path,
    #             "fn": hash,
    #             "blocks": blocks,
    #             "time": time.time(),
    #             "uid": self.fs.ukey(path),
    #         }
    #         self._metadata.update_file(path, detail)
    #         logger.debug("Creating local sparse file for %s", path)

    #     # call target filesystems open
    #     self._mkcache()
    #     f = self.fs._open(
    #         path,
    #         mode=mode,
    #         block_size=block_size,
    #         autocommit=autocommit,
    #         cache_options=cache_options,
    #         cache_type="none",
    #         **kwargs,
    #     )
    #     if self.compression:
    #         comp = (
    #             infer_compression(path)
    #             if self.compression == "infer"
    #             else self.compression
    #         )
    #         f = compr[comp](f, mode="rb")
    #     if "blocksize" in detail:
    #         if detail["blocksize"] != f.blocksize:
    #             raise BlocksizeMismatchError(
    #                 f"Cached file must be reopened with same block"
    #                 f" size as original (old: {detail['blocksize']},"
    #                 f" new {f.blocksize})"
    #             )
    #     else:
    #         detail["blocksize"] = f.blocksize

    #     def _fetch_ranges(ranges):
    #         return self.fs.cat_ranges(
    #             [path] * len(ranges),
    #             [r[0] for r in ranges],
    #             [r[1] for r in ranges],
    #             **kwargs,
    #         )

    #     multi_fetcher = None if self.compression else _fetch_ranges
    #     f.cache = MMapCache(
    #         f.blocksize, f._fetch_range, f.size, fn, blocks, multi_fetcher=multi_fetcher
    #     )
    #     close = f.close
    #     f.close = lambda: self.close_and_update(f, close)
    #     self.save_cache()
    #     return f

    def _parent(self, path):
        return self.fs._parent(path)

    def hash_name(self, path: str, *args: Any) -> str:
        # Kept for backward compatibility with downstream libraries.
        # Ignores extra arguments, previously same_name boolean.
        return self._mapper(path)

    def close_and_update(self, f, close):
        """Called when a file is closing, so store the set of blocks"""
        if f.closed:
            return
        path = self._strip_protocol(f.path)
        self._metadata.on_close_cached_file(f, path)
        try:
            logger.debug("going to save")
            self.save_cache()
            logger.debug("saved")
        except OSError:
            logger.debug("Cache saving failed while closing file")
        except NameError:
            logger.debug("Cache save failed due to interpreter shutdown")
        close()
        f.closed = True
    
    async def _ukey(self, path):
        """Hash of file properties, to tell if it has changed"""
        return sha256(str(await self.fs._info(path)).encode()).hexdigest()

    async def _make_local_details(self, path):
        hash = self._mapper(path)
        fn = os.path.join(self.storage[-1], hash)
        detail = {
            "original": path,
            "fn": hash,
            "blocks": True,
            "time": time.time(),
            "uid": await self._ukey(path),
        }
        self._metadata.update_file(path, detail)
        logger.debug("Copying %s to local cache", path)
        return fn

    async def _cat_file(self, path, start=None, end=None, on_error="raise", callback=DEFAULT_CALLBACK, **kwargs):
        getpath = None
        try:
            # Check if file is in cache
            # This doesnt do anything fancy for chunked data, different chunk ranges
            # are stored in different files even if there is over lap
            path_chunked = path
            if start:
                path_chunked += f"_{start}"
            if end:
                path_chunked += f"_{end}"
            detail = self._check_file(path_chunked)
            if not detail:
                storepath = await self._make_local_details(path_chunked)
                getpath = path
            else:
                detail, storepath = detail if isinstance(detail, tuple) else (None, detail)
        except Exception as e:
            if on_error == "raise":
                raise
            if on_error == "return":
                out = e

        # If file was not in cache, get it using the base file system
        if getpath:
            resp = await self.fs._cat_file(getpath, start=start, end=end)
            # Save to file
            if isfilelike(storepath):
                outfile = storepath
            else:
                outfile = open(storepath, "wb")  # noqa: ASYNC101, ASYNC230
            try:
                outfile.write(resp)
                # IDK yet
                # callback.relative_update(len(chunk))
            finally:
                if not isfilelike(storepath):
                    outfile.close()
            self.save_cache()

        # Call back is weird here, like the progress should be on the file fetch
        # but then how do we deal with the read? Maybe we times it by 2x?
        if start is None:
            start = 0

        callback.set_size(1)
        with open(storepath, "rb") as f:
            f.seek(start)
            if end is not None:
                out = f.read(end - start)
            else:
                out = f.read()
        callback.relative_update(1)
        return out


    def __getattribute__(self, item):
        # TODO: Update
        if item in {
            "load_cache",
            "_open",
            "save_cache",
            "close_and_update",
            "__init__",
            "__getattribute__",
            "__reduce__",
            "_make_local_details",
            "_ukey",
            "open",
            "cat",
            "_cat_file",
            "cat_ranges",
            "get",
            "read_block",
            "tail",
            "head",
            # "info",
            # "ls",
            "exists",
            "isfile",
            "isdir",
            "_check_file",
            "_check_cache",
            "_mkcache",
            "clear_cache",
            "clear_expired_cache",
            "pop_from_cache",
            "local_file",
            "_paths_from_path",
            "get_mapper",
            "open_many",
            "commit_many",
            "hash_name",
            "__hash__",
            "__eq__",
            "to_json",
            "to_dict",
            "cache_size",
            "pipe_file",
            "pipe",
            "start_transaction",
            "end_transaction",
        }:
            # all the methods defined in this class. Note `open` here, since
            # it calls `_open`, but is actually in superclass
            return lambda *args, **kw: getattr(type(self), item).__get__(self)(
                *args, **kw
            )
        if item in ["__reduce_ex__"]:
            raise AttributeError
        if item in ["transaction"]:
            # property
            return type(self).transaction.__get__(self)
        if item in ["_cache", "transaction_type"]:
            # class attributes
            return getattr(type(self), item)
        if item == "__class__":
            return type(self)
        d = object.__getattribute__(self, "__dict__")
        fs = d.get("fs", None)  # fs is not immediately defined
        if item in d:
            return d[item]
        elif fs is not None:
            if item in fs.__dict__:
                # attribute of instance
                return fs.__dict__[item]
            # attributed belonging to the target filesystem
            cls = type(fs)
            m = getattr(cls, item)
            if (inspect.isfunction(m) or inspect.isdatadescriptor(m)) and (
                not hasattr(m, "__self__") or m.__self__ is None
            ):
                # instance method
                return m.__get__(fs, cls)
            return m  # class method or attribute
        else:
            # attributes of the superclass, while target is being set up
            return super().__getattribute__(item)

    def __eq__(self, other):
        """Test for equality."""
        if self is other:
            return True
        if not isinstance(other, type(self)):
            return False
        return (
            self.storage == other.storage
            and self.kwargs == other.kwargs
            and self.cache_check == other.cache_check
            and self.check_files == other.check_files
            and self.expiry == other.expiry
            and self.compression == other.compression
            and self._mapper == other._mapper
            and self.target_protocol == other.target_protocol
        )

    def __hash__(self):
        """Calculate hash."""
        return (
            hash(tuple(self.storage))
            ^ hash(str(self.kwargs))
            ^ hash(self.cache_check)
            ^ hash(self.check_files)
            ^ hash(self.expiry)
            ^ hash(self.compression)
            ^ hash(self._mapper)
            ^ hash(self.target_protocol)
        )
