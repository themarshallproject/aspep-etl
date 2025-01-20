from setuptools import find_packages, setup

setup(
    name="process_aspep",
    packages=find_packages(exclude=["process_aspep_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
