from setuptools import setup

def build():
    setup(
        name = "bnpl",
        version = "0.0.2",
        author = "Brian Abelson",
        author_email = "brianabelson@gmail.com",
        description = "sounds for clubs",
        license = "MIT",
        keywords = "grep",
        url = "https://github.com/abelsonlive/bnpl",
        packages = ['bnpl'],
        package_data={
            'bnpl': ['config/*.yml', 'ext/*/*'],    
        },
        install_requires = [],
        classifiers=[
            "Development Status :: 3 - Alpha",
            "Topic :: Communications :: Email",
            "License :: OSI Approved :: MIT License",
        ]
        # entry_points={
        # 'console_scripts': [
        #     'bnpl = bnpl:run'
        #     ]
        # }
    )

if __name__ == '__main__':
    build()