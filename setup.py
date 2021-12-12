from setuptools import setup
import setuptools

setup(
    name='analyticslib',
    version='1.0',
    packages=setuptools.find_packages(where="src"),
    package_data={
            # If any package contains *.txt or *.rst files, include them:
        "Runner": ["resources/data/*.csv","resources/local_config.yaml"],
        "tests": ["resources/data/*.csv"]
        },
    include_package_data=True,
    package_dir={"": "src"},
)
