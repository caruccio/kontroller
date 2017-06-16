from setuptools import setup

PACKAGE_NAME = "kontroller"
KONTROLLER_VERSION = "0.0.2"
DEVELOPMENT_STATUS = "3 - Alpha"

#with open('requirements.txt') as f:
#    REQUIRES = f.readlines()

with open('kontroller/version.py', 'w') as f:
    f.write('KONTROLLER_VERSION = "{}"'.format(KONTROLLER_VERSION))

setup(
    name=PACKAGE_NAME,
    version=KONTROLLER_VERSION,
    description="Python lib for kubernetes controllers",
    author="Mateus Caruccio",
    author_email="mateus.caruccio@getupcloud.com",
    license="Apache License Version 2.0",
    url="https://github.com/caruccio/kontroller",
    keywords=["Kubernetes"],
#    install_requires=REQUIRES,
    packages=['kontroller'],
    include_package_data=True,
    long_description="""\
    Python lib for kubernetes controllers
    """,
    classifiers=[
        "Development Status :: %s" % DEVELOPMENT_STATUS,
        "Topic :: Utilities",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ],
)
