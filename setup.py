import setuptools

requires = [
    "astropy>=2.0.12",
#    "setuptools>=41.0.0",
    "SQLAlchemy>=1.3.24",
    "redis>=2.10.6",
    "pandas>=0.24.2",
#    "slacker>=0.9.65",
    "numpy>=1.18.5",
#    "credentials>=1.1",
    "python_dateutil>=2.8.0",
    "PyYAML>=5.1.2",
    "mysqlclient>=1.4.4",
    "logger>=1.4"
#    "matplotlib==2.2.5"
]

setuptools.setup(
    name = "meerkat_target_selector",
    version = "0.0.1",
    author = "Tyler Cox",
    author_email = "tyler.a.cox@asu.edu",
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
