from setuptools import find_packages, setup

setup(
    description="Airflow Derived DAG API",
    install_requires=["apache-airflow"],
    name="derived_dag_api",
    packages=find_packages(include=["derived_dag_api"]),
    setup_requires=['setuptools_scm'],
    include_package_data=True,
    version="0.0.1",
    entry_points={
        "airflow.plugins": [
            "DerivedDagApi = derived_dag_api.plugin:DerivedDagApiPlugin"
        ]
    },
)
