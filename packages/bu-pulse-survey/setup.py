#!/usr/bin/env python
from setuptools import setup


def requirements(path):
    with open(path, "r") as fh:
        found = [
            package[: package.find("#")]
            for package in fh.readlines()
            if package and not package.startswith("#")
        ]

    return found


install_requirements = requirements("requirements.txt")

setup(
    name="bu-pulse-survey",
    version="0.1.0",
    description="Tool for identifing and sending surveys to new members.",
    author="BetterUp Analytics",
    url="",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["bu_pulse_survey"],
    install_requires=install_requirements,
    entry_points="""
    [console_scripts]
    send-first-pulse-survey=bu_pulse_survey.first_pulse:main
    """,
    packages=["bu_pulse_survey"],
    include_package_data=True,
)
