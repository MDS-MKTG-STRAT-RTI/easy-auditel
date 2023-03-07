import sys

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

pyth_version = (sys.version_info.major, sys.version_info.minor)

np_python_37 = ["numpy~=1.21.5", "pandas~=1.3.5"]
np_python_ge38 = ["numpy>=1.22.1", "pandas>=1.4.0"]

if pyth_version == (3, 7):
    numpy_pandas_version = np_python_37
elif pyth_version >= (3, 8):
    numpy_pandas_version = np_python_ge38
else:
    print("Not supported python version (>=3.7 required)")
    sys.exit(1)


spark_libs = numpy_pandas_version + ["pyspark>=3.1.1"]

setuptools.setup(
    name="easyauditel",
    version="0.0.1",
    author="Mirko Leccese",
    author_email="mirko.leccese@mediaset.it",
    description="Mediaset Libraries",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/MDS-MKTG-STRAT-RTI/easy-auditel",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7"
)
