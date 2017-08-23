MOiSt-Airflow
=============


Model Output (integrated) Statistics based on airflow
-----------------------------------------------------
MOiSt-Airflow is a package of airflow operators and direct acyclic graphs to
post-process meteorological data for the purpose of model output statistics
(MOS). This package will be used to generate the forecast for a measurement
station in Hamburg (Germany) called "Wettermast Hamburg". Also this package
could be seen as successor of the
`pymepps-streaming <https://github.com/maestrotf/pymepps-streaming>`_ package.

At the moment the
`Wettermast Hamburg forecast
<http://wettermast.uni-hamburg.de/frame.php?doc=Vorhersage.htm>`_ is based on
another script collection, due to missing features. In the future this package
is filled with life and will contain more features.


Installation
------------
This package is programmed to use the airflow framework for the execution of the
pipeline. This project is also based on
`pymepps <https://github.com/maestrotf/pymepps>`_, which needs to be installed
for the full feature set. The package is programmed in Python 3.6, so it should
be possible to use Python 3.


Clone package
^^^^^^^^^^^^^
This package is not a valid python package. So this package could not be found
on pypi and conda and it is necessary to clone this package.

.. code:: sh

    git clone git@github.com:maestrotf/moist_airflow.git


Install requirements via conda environment (recommended)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The environment.yml could be used as base environment for this package. The
environment is based on Python 3.6, Airflow 1.8 and pymepps 0.4. This is the
recommended way to install the requirements.

.. code:: sh

    cd moist_airflow
    conda env create -f environment.yml
    source activate moist_airflow

To use airflow subpackages they need to be installed into the environment. The
following is an example to install the mysql operators and hooks

.. code:: sh

    source activate moist_airflow
    pip install apache-airflow[mysql]


Install requirements via pip
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Beside the conda environment it is also possible to install the requirements
via pip.

.. code:: sh

    cd moist_airflow
    pip install -r requirements.txt


Operators
^^^^^^^^^
The operators are installed as moist_airflow. This package has to be installed
to use the operators.

.. code:: sh

    cd moist_airflow
    pip install .

The operators are imported as moist_airflow

.. code:: python

    import moist_airflow        # Line to import the operators
    example_task = moist_airflow.example_sensor()


Plugins
^^^^^^^
To use the plugins the operators have to be installed previously. The plugins
have to be moved to the plugins folder of $AIRFLOW_HOME.

.. code:: sh

    cd moist_airflow/plugins
    cp *.py $AIRFLOW_HOME/plugins


DAGS
^^^^
To use the pre-defined dags the operators and plugins have to be installed. The
dag python files need to moved into the dags folder of $AIRFLOW_HOME. To see the
dags it is necessary to restart the airflow web server.

.. code:: sh

    cd moist_airflow/dags
    cp *.py $AIRFLOW_HOME/dags/


Authors
-------
* **Tobias Finn** - *Initial creator* - `maestrotf <https://github.com/maestrotf>`_

License
-------
This project is licensed under the GPL3 License - see the
`license <LICENSE.md>`_ file for details.
