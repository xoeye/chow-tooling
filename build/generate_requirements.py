#!/usr/bin/env python
import hashlib
import json
import logging
import os
import subprocess
import typing as ty

from hashdeep import hashdeep
from setuptools import find_packages

REQUIREMENTS_HASH_PREFIX = "# sha256hex: "


logger = logging.getLogger(f"{os.path.basename(os.getcwd()):30}")


def sha256file(filename):
    """Memory-efficient SHA256 sum in pure Python"""
    sha256_hash = hashlib.sha256()
    with open(filename, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
        hexdigest = sha256_hash.hexdigest()
        return hexdigest


def _pipenv_sources(pipfile_lock_data: dict) -> ty.List[str]:
    out_lines = []
    for source in pipfile_lock_data.get("_meta", {}).get("sources", []):
        url = source["url"]
        if source["name"] == "pypi":
            out_lines.append(f"-i {url}")
        else:
            out_lines.append(f"--extra-index-url {url}")
    return out_lines


def _pipenv_requirements_section(pipenv_lock_data, section) -> list:
    from pipenv.vendor.requirementslib import Requirement

    # this is imported late because it very rudely does a basicConfig on logging that suppresses all our logs
    reqs = list()
    for name, entry in pipenv_lock_data[section].items():
        r = Requirement.from_pipfile(name, entry)
        reqs.append(r)
    return reqs


def pipenv_lock(source_path, sections=("default",), **requirements_kwargs) -> ty.List[str]:
    """If you rely on `pipenv lock -r to do this for you, it will force-create a venv for you.

    This is a workaround that gives the same result.

    You'll want to '\n'.join() the result list and write it to a file.
    """
    with open(source_path) as f:
        data = json.load(f)
        out_lines = _pipenv_sources(data)
        all_reqs = dict()
        for section in sections:
            for r in _pipenv_requirements_section(data, section):
                all_reqs[r.name] = r
        return out_lines + [r.as_line(**requirements_kwargs) for r in all_reqs.values()]


def _get_saved_requirements_hash(reqs_txt: str) -> str:
    first_line = open(reqs_txt).read().splitlines()[0]
    if REQUIREMENTS_HASH_PREFIX not in first_line:
        return ""
    required_source_hash = first_line[len(REQUIREMENTS_HASH_PREFIX) :].strip()
    return required_source_hash


def requirements_hash_matches(lockfile: str, source_hash: str) -> bool:
    if not os.path.exists(lockfile):
        logger.info(f"Lockfile {lockfile} does not exist")
        return False
    required_source_hash = _get_saved_requirements_hash(lockfile)
    if not required_source_hash:
        logger.info(
            f"Lockfile {lockfile} exists and has no hash - "
            "assuming this is a developer-generated file and not replacing it!"
        )
        return True
    if not source_hash == required_source_hash:
        logger.info(
            f"Lockfile exists but actual file hash {source_hash} does not match"
            f" hash from requirements file {required_source_hash}"
        )
        return False
    return True


def poetry_lock(source_path: str, *_args, **_kw) -> ty.List[str]:
    reqs_txt = subprocess.check_output(["poetry", "export", "-f", "requirements.txt"]).decode(
        "utf-8"
    )
    return reqs_txt.split("\n")


PYTHON_REQS_EXPORTERS = {"Pipfile.lock": pipenv_lock, "poetry.lock": poetry_lock}


def _hash_str(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def _hashdeep_any_editable_deps(lines: ty.Iterable[str]) -> ty.Iterator[str]:
    for line in lines:
        if line.startswith("-e "):
            path = line.split(" ")[1]
            if path.startswith("git+"):
                yield path
            else:
                pkgs = find_packages(path)
                main_pkg = pkgs[0]
                yield "# deep hash of below editable dep: " + _hash_str(
                    hashdeep(main_pkg, ".*__pycache__.*")
                )
        yield line


def generate_requirements_from_lock_source(source_file: str, requirements_txt: str) -> str:
    working_dir = os.path.dirname(os.path.abspath(requirements_txt))
    abs_source_file = working_dir + os.sep + source_file
    cwd = os.getcwd()
    os.chdir(working_dir)
    try:
        dev = "-dev" in requirements_txt
        logger.info(
            f'Generating {"dev" if dev else "release"} lockfile {requirements_txt} from {abs_source_file}...'
        )
        lines = PYTHON_REQS_EXPORTERS[source_file](  # type: ignore
            abs_source_file,
            ["default", "develop"] if dev else ["default"],
            include_hashes=False,
            # this is a long story, but basically when we use requirements.txt files
            # with hashes it runs up against https://github.com/pypa/pip/issues/4995 .
            # We had a workaround that worked great - split up each dependency
            # into its own separate mini-requirements file - but that stopped working when
            # we hit our first "namespaced package" (firebase_admin); basically we didn't
            # actually get all of the necessary dependencies installed.
        )
        # solve for editable deps
        lines = list(_hashdeep_any_editable_deps(lines))

        return "\n".join(lines) + "\n"
    finally:
        os.chdir(cwd)


def ensure_lockfile(lockfile: str, lockfile_source: str = "Pipfile.lock"):
    """This really only applies to Pipfiles at this point"""
    new_lockfile_text = generate_requirements_from_lock_source(lockfile_source, lockfile)

    if new_lockfile_text:
        logger.info(f"Writing lockfile text to {lockfile}")
        with open(lockfile, "w") as outf:
            outf.write(new_lockfile_text)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--lockfile")
    parser.add_argument("--hash-source")
    args = parser.parse_args()

    if args.lockfile:
        ensure_lockfile(os.path.abspath(args.lockfile))
    elif args.hash_source:
        print(REQUIREMENTS_HASH_PREFIX + sha256file(args.hash_source))


if __name__ == "__main__":
    main()
