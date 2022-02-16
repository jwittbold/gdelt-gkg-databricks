import setuptools

with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()

setuptools.setup(
    name='gdelt-gkg',
    version='1.0.0',
    author='Jack Wittbold',
    author_email='jwittbold@gmail.com',
    description='ETL Pipeline for GDELT GKG Files',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/jwittbold/gdelt-gkg',
    project_urls={
        'The GDELT Project': 'https://www.gdeltproject.org',
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
    ],
    package_dir={'': 'src'},
    packages=setuptools.find_packages(where='src'),
    python_requires='>=3.8',
)

