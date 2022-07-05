#!/usr/bin/env python
"""Pip has a weird issue where it refuses to install a package with no
--hash specification if there are different packages with a --hash
specification in the same requirements.txt.

https://github.com/pypa/pip/issues/4995

This indirectly works around that by installing each concrete requirement
in a completely separate process.

It extracts all the shared flags from the source requirements file and puts
them all into a fake requirements file along with one dependency at a time.

This has the additional effect of causing installs to run in parallel, which is quite fast!

It currently only works on *nix systems with /dev/stdin, though this could be changed
to writing a temp requirements file if desired.
"""
import typing as ty

import argparse
import subprocess
import os
import multiprocessing.pool


MAX_PARALLEL_PIPS = int(os.environ.get("MAX_PARALLEL_PIPS", 20))


def yield_pip_requirements_lines(reqs_txt: str) -> ty.Iterable[str]:
    """Also drops comments"""
    accum = ""
    for line in reqs_txt.splitlines():
        if line.startswith("#"):
            continue
        if line.startswith("-e "):
            line = line[
                3:
            ]  # because of https://github.com/pypa/pip/issues/4390, we can't use '-e ' and --target at the same time
        if line.endswith("\\"):
            accum += line[-1] + " "
        else:
            yield accum + line
            accum = ""


def is_config_line(line: str) -> bool:
    return line.startswith("-") and not line.startswith("-e")


def yield_mini_reqs_txts(reqs_txt: str) -> ty.Iterable[str]:
    all_lines = list(yield_pip_requirements_lines(reqs_txt))
    config_lines = list()
    reqs_lines = list()
    for line in all_lines:
        if is_config_line(line):
            config_lines.append(line)
        else:
            reqs_lines.append(line)
    for req in reqs_lines:
        yield "\n".join(config_lines + [req])


def pip_install_target_no_deps(target_dir: str, reqs_txt: str, *pip_args):
    """Only works on *nix for now"""
    cmd = ["pip", "install", "-t", target_dir, "--no-deps", "-r", "/dev/stdin", *pip_args]
    proc = subprocess.run(cmd, input=reqs_txt, text=True)
    if proc.returncode:
        print("Failed " + " ".join(cmd) + " with input:")
        print(reqs_txt)
        print("<<<>>>")
    return proc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("target_directory")
    parser.add_argument("source_requirements")
    args, unk_args = parser.parse_known_args()

    target_dir = os.path.abspath(args.target_directory)
    source_reqs = os.path.abspath(args.source_requirements)

    os.makedirs(target_dir, exist_ok=True)

    individual_reqs = list(yield_mini_reqs_txts(open(source_reqs).read()))

    def install_closure(reqs_txt: str):
        return pip_install_target_no_deps(target_dir, reqs_txt, *unk_args)

    # os.chdir(os.path.dirname(source_reqs))  # so relative paths will work
    procs = multiprocessing.pool.ThreadPool(min(MAX_PARALLEL_PIPS, len(individual_reqs))).map(
        install_closure, individual_reqs
    )

    return any([proc.returncode for proc in procs])


if __name__ == "__main__":
    import sys

    sys.exit(main())