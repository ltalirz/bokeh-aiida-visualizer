#!/usr/bin/env python

from __future__ import absolute_import
from setuptools import setup, find_packages

if __name__ == '__main__':
    # Provide static information in setup.json
    # such that it can be discovered automatically
    setup(
        # Note: For actual installation, need to add packages to MANIFEST.in!
        packages=find_packages(),
        name="bokeh-aiida-visualizer",
        author="Leopold Talirz",
        author_email="info@materialscloud.org",
        description="Visualizer written for AiiDA molsim tutorial 2019",
        license="MIT",
        classifiers=["Programming Language :: Python"],
        version="0.1.0",
        install_requires=[
            "bokeh>=0.13", "jsmol-bokeh-extension", "pandas", "sqlalchemy"
        ],
        extras_require={
            "pre-commit": [
                "pre-commit==1.11.0", "yapf==0.24.0", "prospector==0.12.11",
                "pylint==1.9.3"
            ]
        })
