"""
Setup configuration for churn-classification-mlops package
"""

from setuptools import setup, find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory/"README.md").read_text()

requirements = (this_directory/"requirements.txt").read_text().splitlines()

setup(
    name='churn-classification-mlops',
    version="1.0.0",
    author='Arya',
    author_email="Aryaganendra45@gmail.com",
    description="MLOps project for customer churn prediction with Docker and DVC",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/churn-classification-mlops",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev" : [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "ruff>=0.1.0",
            "black>=23.0.0",
            "jupyter>=1.0.0",
        ],
        "dvc" : [
            "dvc[s3]>=3.30.0",
            "dvc[gdrive]>=3.30.0",
        ],
    },
    entry_points={
        "console_scripts":[
            "churn-preprocess=src.preprocess:main",
            "churn-train=src.train:main",
            "churn-evaluate=src.evaluate:main",
            "churn-predict=src.predict:main",
        ]
    },
    include_package_data=True,
    zip_safe=False
)