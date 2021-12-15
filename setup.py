from setuptools import setup, find_packages
import pathlib
import py_import_tree
# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="py_import_tree",
    version=py_import_tree.__version__,
    description="py_import_tree: A library for analyzing Python's code tree.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/hristo-vrigazov/py-import-tree",
    author="Hristo Vrigazov",
    author_email="hvrigazov@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent"
    ],
    packages=find_packages(exclude=("tests",)),
    include_package_data=True,
    install_requires=["pandas",
                      "numpy",
                      "stdlib_list",
                      "astunparse",
                      "joblib"],
    extras_require={},
    data_files=[
        ('schema', ['py_import_tree/schema.sql'])
    ]
)
