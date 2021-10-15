import os

from setuptools import find_packages, setup

dpath = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(dpath, "README.md"), "r") as f:
    long_description = f.read()


setup(
    name="isqlite",
    version="0.11",
    description="An improved Python interface to SQLite",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="Ian Fisher",
    author_email="iafisher@fastmail.com",
    entry_points={"console_scripts": ["isqlite = isqlite.cli:cli"]},
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "click >= 8.0.0",
        "sqliteparser >= 0.2.5",
        "tabulate >= 0.8.9",
        "xcli >= 0.2",
    ],
    project_urls={"Source": "https://github.com/iafisher/isqlite"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: SQL",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Topic :: Database",
    ],
)
