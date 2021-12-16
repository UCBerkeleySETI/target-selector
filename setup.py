import setuptools

requires = [
    # "astropy>=2.0.12",
    # "setuptools>=41.0.0",
    "SQLAlchemy>=1.3.24",
    "redis>=2.10.6",
    "pandas>=0.24.2",
    # "slacker>=0.9.65",
    "numpy>=1.18.5",
    # "credentials>=1.1",
    "python_dateutil>=2.8.0",
    "PyYAML>=5.1.2",
    "mysqlclient>=1.4.4",
    "logger==1.4",
    "mip>=1.13.0",
    # "matplotlib==2.2.5",
    "scipy>=1.4.1",
    "scikit-image>=0.15.0",
    "pytz>=2021.1",
    "backports-datetime-fromisoformat==1.0.0"
]

setuptools.setup(
    name = "meerkat_target_selector",
    version = "0.0.2",
    author = "Bart Wlodarczyk-Sroka, Kevin Lacker, Daniel Czech, Tyler Cox",
    author_email = "bart.wlodarczyk-sroka@postgrad.manchester.ac.uk",
    description = ("Breakthrough Listen's MeerKAT Target Selector"),
    license = "MIT",
    keywords = "example documentation tutorial",
    long_description=open("README.md").read(),
    install_requires=requires,
    packages=[
        'mk_target_selector'
        ],
    py_modules = [
        'target_selector_start',
        ],
    entry_points = {
        'console_scripts': [
            'target_selector_start = target_selector_start:cli',
        ]
    },
)
