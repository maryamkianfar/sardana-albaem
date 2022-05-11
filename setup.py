#!/usr/bin/env python

###############################################################################
#     sardana-ctrl-albaem
#
#     Copyright (C) 2019  MAX IV Laboratory, Lund Sweden.
#
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see [http://www.gnu.org/licenses/].
###############################################################################

from setuptools import setup, find_packages


def main():
    """Main method collecting all the parameters to setup."""
    name = "sardana-albaem"

    description = "AlbaEM Sardana Controller"

    author = "kits"

    author_email = "kitscontrols@maxiv.lu.se"

    license = "GPLv3"

    url = "https://gitlab.maxiv.lu.se/kits-maxiv/sardana-albaem"

    packages = find_packages()

    # Add your dependencies in the following line.
    install_requires = [
        "sardana",
        "sockio",
        "pyzmq<20.0; python_version<'3'",
        "pyzmq; python_version>='3'",
    ]

    python_requires = ">=2.7"

    setup(
        name=name,
        use_scm_version=True,
        setup_requires=["setuptools_scm"],
        description=description,
        author=author,
        author_email=author_email,
        license=license,
        url=url,
        packages=packages,
        install_requires=install_requires,
        python_requires=python_requires,
    )

if __name__ == "__main__":
    main()
