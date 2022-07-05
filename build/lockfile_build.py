#!/usr/bin/env python
"""Takes a Pipenv or yarn lockfile, and returns an S3 path containing
the built and uploaded result of that lockfile.

This MUST ALWAYS be run with the current working directory
as the directory of the lockfile.
"""
import argparse
import hashlib
import logging
import os
import platform
import shutil
import subprocess

# pylint: disable=logging-fstring-interpolation
import typing as ty
from functools import partial
from zipfile import ZIP_DEFLATED, ZipFile

import boto3
import botocore

# also requires pipenv if you want to do stuff with Python


def _dir_name():
    return f"{os.path.basename(os.getcwd()):30}"


logger = logging.getLogger(_dir_name())


_ARTIFACT_PATHS_SAFELY_IGNORED = {"__pycache__"}

EMPTY_FILE_HASH = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
ARTIFACT_HASH_HINT = ".ARTIFACT_HASH.txt"

COMPRESS_CMDS = {"tgz": "tar czf {outf} {uf}"}
UNCOMPRESS_CMDS = {"tgz": "tar xfp {cf}"}


def get_lockfile_result_directory(lockfile: str) -> str:
    if lockfile in {"yarn.lock", "package-lock.json"}:
        return "node_modules"
    if lockfile.startswith("requirements"):
        if "-dev" in lockfile:
            return "python-dev"
        return "python"
    raise ValueError(f"Unknown lockfile name {lockfile}")


def get_lockfile_install_operations(lockfile: str) -> ty.List[str]:
    if lockfile == "yarn.lock":
        return ["yarn --frozen-lockfile"]
    if lockfile == "package-lock.json":
        return ["npm install"]
    if lockfile.startswith("requirements"):
        # https://www.python.org/dev/peps/pep-0571/#backwards-compatibility-with-manylinux1-wheels
        # we use manylinux2010 because some packages are dropping support for the older
        # manylinux1, and in general we'd rather use precompiled wheels rather than
        # compiling them locally.
        # Packages which don't provide a manylinux2010 wheel but do provide a manylinux1 wheel
        # will still be installed by pip as manylinux2010.
        plat = "--platform=manylinux2010_x86_64" if "-dev" not in lockfile else ""
        result_dir = get_lockfile_result_directory(lockfile)
        return [
            f"../scripts/pip_562_requirements_target_install.py -t {result_dir} -r {lockfile} {plat}"
        ]
        # return [f"../scripts/pip_4995_target_dir_no_deps_install.py {result_dir} {lockfile} {plat}"]
        # parallel nodeps install does not work with namespaced packages for some reason... :(
    raise ValueError(f"Unknown lockfile {lockfile}")


def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))


def compress(op: str, uf: str) -> str:
    outf = f"{uf}.{op}.tmp"
    result = f"{uf}.{op}"
    if op == "zip":
        logger.info(f"Zipping {uf} to {result}")
        with ZipFile(result, mode="w", compression=ZIP_DEFLATED) as zipf:
            zipdir(uf, zipf)
    else:
        logger.info(f"Running {op} on {uf}")
        subprocess.check_call(
            COMPRESS_CMDS[op].format(uf=uf, outf=outf).split(" "), stdout=subprocess.DEVNULL
        )
        result = f"{uf}.{op}"
        os.rename(outf, result)
    logger.info(f"Result file is {os.path.getsize(result)} bytes")
    return result


def uncompress(op: str, cf: str):
    if op == "zip":
        with ZipFile(cf) as zipf:
            zipf.extractall()
    else:
        subprocess.check_call(
            UNCOMPRESS_CMDS[op].format(cf=cf).split(" "), stdout=subprocess.DEVNULL
        )


DEFAULT_S3_PREFIX = "locked_builds"

S3_CLIENT = boto3.client("s3", config=botocore.client.Config(signature_version="s3v4"))


def is_result_mtime_gte(result_mtime: float, source_path: str) -> bool:
    source_mtime = os.path.getmtime(source_path)
    return result_mtime >= source_mtime


def make_is_result_up_to_date(result_path: str) -> ty.Callable[[str], bool]:
    if not os.path.exists(result_path):
        logger.info(f"Result path {result_path} does not exist")
        return lambda s: False
    return partial(is_result_mtime_gte, os.path.getmtime(result_path))


def sha256file(filename):
    """Memory-efficient SHA256 sum in pure Python"""
    sha256_hash = hashlib.sha256()
    with open(filename, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
        hexdigest = sha256_hash.hexdigest()
        return hexdigest


def does_build_exist(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError:
        pass
    return False


def clear_result(lockfile: str):
    Dir = get_lockfile_result_directory(lockfile)
    if os.path.exists(Dir):
        shutil.rmtree(Dir)


def compression_op(result_dir: str) -> str:
    return "zip" if result_dir.startswith("python") else "tgz"


def result_artifact(result_dir):
    return result_dir + f".{compression_op(result_dir)}"


def compress_directory(result_dir):
    return compress(compression_op(result_dir), result_dir)


def artifact_hint_path(result_dir: str) -> str:
    return f"{result_dir}/{ARTIFACT_HASH_HINT}"


def write_artifact_hint(result_dir: str, artifact_sha: str):
    logger.info(f'Writing artifact hint to "{result_dir}"')
    with open(artifact_hint_path(result_dir), "w") as f:
        f.write(artifact_sha)


def is_result_dir_clean(result_dir: str, artifact_sha: str) -> bool:
    art_hint_path = artifact_hint_path(result_dir)
    if not os.path.exists(art_hint_path):
        logger.info(f"Artifact hint does not exist at {art_hint_path}")
        return False
    art_hint_sha = open(art_hint_path).read().rstrip("\n")
    if art_hint_sha != artifact_sha:
        logger.info(f"Artifact hint sha {art_hint_sha} != {artifact_sha}")
        return False
    is_artifact_up_to_date = make_is_result_up_to_date(art_hint_path)
    files = os.listdir(result_dir)
    for path in files:
        path = os.path.join(result_dir, path)
        if path != art_hint_path:
            if not is_artifact_up_to_date(path):
                logger.info(f"Artifact appears out of date because of {path}")
                return False
    logger.info(
        f'Result directory "{result_dir}" has not been modified since the artifact hint was placed'
    )
    return True


def retry_value_error_n_times(n: int):
    def retry_value_error(func):
        def wrapped(*args, **kwargs):
            attempts = 0
            while attempts < n:
                try:
                    return func(*args, **kwargs)
                except ValueError as ve:
                    logger.info(f"Retrying {func} after {ve}")
                    attempts += 1
                    if attempts >= n:
                        raise ve

        return wrapped

    return retry_value_error


def get_artifact_hash(artifact: str) -> str:
    if os.path.exists(artifact):
        logger.info(f"{artifact} exists locally - getting SHA")
        return sha256file(artifact)
    logger.warning("No local artifact exists")
    return ""


def use_prebuilt(s3_client, s3_bucket: str, key: str, lockfile: str, lockfile_hash: str) -> str:
    """Only call this if the build already exists remotely"""
    obj = s3_client.get_object(Bucket=s3_bucket, Key=key)
    obj_sha = obj.get("Metadata", {}).get("sha256hex", "")
    if not obj_sha:
        logger.warning("Object is missing SHA - that is not a great sign")

    result_dir = get_lockfile_result_directory(lockfile)
    artifact = result_artifact(result_dir)
    artifact_sha = get_artifact_hash(artifact)
    if not os.path.exists(artifact) or not obj_sha or obj_sha != artifact_sha:

        @retry_value_error_n_times(3)
        def download_s3_artifact_and_check_sha():
            msg = f"Downloading pre-built artifact from {s3_bucket} for {lockfile} to {os.path.abspath(artifact)} "
            obj = s3_client.get_object(Bucket=s3_bucket, Key=key)
            obj_sha = obj.get("Metadata", {}).get("sha256hex", "")
            original_workdir = obj.get("Metadata", {}).get("workdir", "")
            if original_workdir:
                msg += f"with origin workdir {original_workdir} "
            msg += f"with hash {lockfile_hash}"
            logger.info(msg)
            with open(artifact, "wb") as fileobj:
                s3_client.download_fileobj(s3_bucket, key, fileobj)
            artifact_sha = sha256file(artifact)
            if obj_sha and obj_sha != artifact_sha:
                raise ValueError(
                    f"Downloaded file SHA is incorrect! {artifact} {obj_sha} {artifact_sha}"
                )
            return artifact_sha

        artifact_sha = download_s3_artifact_and_check_sha()
    else:
        logger.info("artifact SHA matched - skipping download")

    if not is_result_dir_clean(result_dir, artifact_sha):
        clear_result(lockfile)
        artifact_size = os.path.getsize(artifact)
        logger.info(f"Uncompressing artifact {artifact} of size {artifact_size} locally for use")
        uncompress(compression_op(result_dir), artifact)
        write_artifact_hint(result_dir, artifact_sha)
    return result_dir


def install_from_lockfile(lockfile: str) -> str:
    logger.info(f"Installing from {lockfile}")
    install_cmds = get_lockfile_install_operations(lockfile)
    for bc in install_cmds:
        print(bc)
        rc = os.system(bc)
        if rc:
            raise ValueError(f'Failed "{bc}" with code {rc}')
    result_dir = get_lockfile_result_directory(lockfile)
    results = os.listdir(result_dir)
    logger.info(f"Installed {len(results)} items into {result_dir}")
    return result_dir


def artifact_locally_and_on_s3_from_lockfile(
    s3_client,
    s3_bucket: str,
    key_prefix: str,
    lockfile: str,
    fail_without_prebuilt: bool = False,
    overwrite: bool = False,
    skip_upload: bool = False,
) -> str:
    """Finds and downloads a build artifact on S3 if it exists. If not, creates it and uploads it.

    Either way you end up with a local copy of the build artifact.

    Returns the string path of the local, uncompressed build, which should
    be one of the options defined in LOCKFILE_RESULT_DIRECTORY.
    """
    lockfile_hash = sha256file(lockfile)
    logger.info(f"Hash of lockfile {lockfile} is {lockfile_hash}")
    assert lockfile_hash != EMPTY_FILE_HASH
    key_prefix = key_prefix.rstrip("/") + "/"
    key = key_prefix + lockfile_hash

    if does_build_exist(s3_client, s3_bucket, key) and not overwrite:
        logger.info(f"Build already exists for {lockfile} with hash {lockfile_hash}")
        return use_prebuilt(s3_client, s3_bucket, key, lockfile, lockfile_hash)
    else:
        logger.info(f"Build does not exist in {s3_bucket} for {lockfile} with {lockfile_hash}")
        if fail_without_prebuilt:
            raise ValueError(f"Build artifact does not exist in {s3_bucket} at {key}")

    # we need to do a perfectly clean build
    logger.info("Clearing existing builds for a clean build")
    clear_result(lockfile)

    logger.info(f"Creating built directory for {lockfile} with hash {lockfile_hash}")
    result_dir = install_from_lockfile(lockfile)
    logger.info(f"Compressing {result_dir} to artifact")
    artifact = compress_directory(result_dir)
    artifact_sha = sha256file(artifact)
    write_artifact_hint(result_dir, artifact_sha)

    if not skip_upload and s3_client and s3_bucket:
        try:
            logger.info(f"uploading built artifact to {s3_bucket} : {key}")
            s3_client.head_bucket(Bucket=s3_bucket)
            s3_client.upload_file(
                artifact,
                s3_bucket,
                key,
                dict(
                    Metadata=dict(
                        sha256hex=artifact_sha,
                        workdir=os.path.basename(os.path.dirname(os.path.abspath(lockfile))),
                    )
                ),
            )
        except botocore.client.ClientError as ce:
            logger.info(f"{s3_bucket} does not exist: " + str(ce))
    return result_dir


def artifact_built_dir_from_lockfile(lockfile: str) -> str:
    existing_result_dir = get_lockfile_result_directory(lockfile)
    existing_artifact = result_artifact(existing_result_dir)
    existing_artifact_sha = get_artifact_hash(existing_artifact)
    if is_result_dir_clean(existing_result_dir, existing_artifact_sha):
        return existing_result_dir
    return ""


def artifact_from_lockfile(lockfile: str) -> str:
    result_dir = install_from_lockfile(lockfile)
    if result_dir == "python":
        logger.info(f"Compressing {result_dir} to create artifact")
        # we need a ZIP for serverless deployments
        compress_directory(result_dir)
    return result_dir


def default_key_prefix(
    lockfile: str,
) -> str:
    """Not / terminated"""
    default_key_parts = [
        DEFAULT_S3_PREFIX,
        get_lockfile_result_directory(lockfile),
        platform.system(),
    ]
    key_prefix = "/".join(default_key_parts)
    return key_prefix


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lockfile")
    parser.add_argument(
        "--s3-bucket",
        "-b",
        help="If not provided, the build will be run directly"
        + " and no additional work will be done",
    )
    parser.add_argument(
        "--skip-upload", action="store_true", help="Don't upload the built artifact to S3"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="If your S3 build is corrupt, this will force-replace it",
    )
    parser.add_argument(
        "--fail-without-prebuilt",
        action="store_true",
        help="If a build doesn't already exist on S3, consider this an error",
    )
    args = parser.parse_args()

    lockfile = args.lockfile

    global logger
    logger = logging.getLogger(_dir_name() + f" : {lockfile:20}")
    logger.info(args)

    try:
        # developer convenience
        from lockfile_build_default_bucket import default_bucket
    except ImportError as ie:
        logger.warning(ie)

        def default_bucket():
            return ""

    bucket = args.s3_bucket or default_bucket()

    if lockfile.startswith("requirements"):
        # generating an intermediate lockfile only applies to Python
        try:
            import generate_requirements

            if os.path.exists("poetry.lock"):
                generate_requirements.ensure_lockfile(lockfile, "poetry.lock")
            elif os.path.exists("Pipfile.lock"):
                generate_requirements.ensure_lockfile(lockfile, "Pipfile.lock")
        except ImportError as ie:
            logger.warning(ie)

    # test python path to keep myself sane
    ppath = os.environ.get("PYTHONPATH", "")
    if "python" in ppath or "python-dev" in ppath:
        logger.warning(
            "Your PYTHONPATH is going to cause your artifacts to be unzipped every time "
            "because __pycache__ will get modified. You may be able to set "
            "PYTHONDONTWRITEBYTECODE to disable bytecode creation."
        )

    if artifact_built_dir_from_lockfile(lockfile) and not args.overwrite:
        print("Your artifact and built directory are already up to date.")
        return

    if bucket:
        artifact_locally_and_on_s3_from_lockfile(
            S3_CLIENT,
            bucket,
            default_key_prefix(lockfile),
            lockfile,
            args.fail_without_prebuilt,
            args.overwrite,
            args.skip_upload,
        )
    elif not args.fail_without_prebuilt:
        artifact_from_lockfile(lockfile)
    else:
        raise ValueError(
            "Cannot specify --fail-without-prebuilt without providing a bucket to get the build from!"
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()