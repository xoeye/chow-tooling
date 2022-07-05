#!/usr/bin/env python
import argparse
import os
import subprocess


def _remove_all_editable(reqs_txt: str):
    # this is a horrible hack, but without it we run afoul of pip bug https://github.com/pypa/pip/issues/562
    def _remove_prefix(prefix, s):
        if s.startswith(prefix):
            return s[len(prefix) :]
        return s

    return "\n".join([_remove_prefix("-e ", line) for line in reqs_txt.splitlines()])


def pip_install_target_no_deps_no_editables(target_dir: str, reqs_txt: str, *pip_args):
    """Only works on *nix for now"""
    cmd = ["pip", "install", "-t", target_dir, "--no-deps", "-r", "/dev/stdin", *pip_args]
    reqs_txt = _remove_all_editable(reqs_txt)
    proc = subprocess.run(cmd, input=reqs_txt, text=True)
    if proc.returncode:
        print("Failed " + " ".join(cmd) + " with input:")
        print(reqs_txt)
        print("<<<>>>")
    return proc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--target", required=True)
    parser.add_argument("-r", "--requirement", required=True)
    args, unk_args = parser.parse_known_args()

    target = os.path.abspath(args.target)
    requirement = os.path.abspath(args.requirement)

    os.makedirs(target, exist_ok=True)

    return pip_install_target_no_deps_no_editables(
        target, open(requirement).read(), *unk_args
    ).returncode


if __name__ == "__main__":
    import sys

    sys.exit(main())