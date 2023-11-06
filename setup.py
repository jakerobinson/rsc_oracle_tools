from setuptools import find_packages, setup
setup(
    name='rscOracleTools',
    packages=find_packages(include=['rsc_oracle']),
    version='0.1.0',
    description='Package of tools to use Rubrik GraphQL API for Oracle',
    author='Julian Zgoda',
    license='MIT',
    install_requires=[
        'requests >= 2.18.4, != 2.22.0',
        'urllib3 >= 1.26.5',
        'gql',
        'Click',
        'pytz',
        'yaspin',
        'tabulate'
    ],
)
