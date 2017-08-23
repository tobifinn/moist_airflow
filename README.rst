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
`pymepps <https://github.com/maestrotf/pymepps>`_. So if you want to use the
whole feature set of this package you have to install also pymepps.

Authors
-------
* **Tobias Finn** - *Initial creator* - `maestrotf <https://github.com/maestrotf>`_

License
-------
This project is licensed under the GPL3 License - see the
`license <LICENSE.md>`_ file for details.
