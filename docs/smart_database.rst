.. _smart_database:

SMART Database
==============

Relationship to mwa_search
--------------------------

The SMART search pipeline as implemented in ``mwa_search`` (for the second pass and beyond) is inextricably intertwined with the `SMART Database and Webapp <https://github.com/ADACS-Australia/SS2020A-RBhat>`_ (private GitHub repository), which hereafter will be referred to as "the Database".
Versions of the Database from v2.0 onward have been designed with generality in mind, and can potentially be used by pulsar surveys other than SMART, with all the elements that *define* the SMART survey being encapsulated by the **data** in the Database, rather than by the **structure** of the Database.
For an overview of the Database structure itself, refer to the Database documentation directly [TODO: add link once published].

v4.0 of ``mwa_search`` is the first version that will use the new Database structure defined in v2.0 (of the Database).
And just as the Database is on a trajectory towards more general usage (outside of SMART), so too are future versions of ``mwa_search`` destined to be more general as well.
This is possible to the extent to which the above-mentioned paradigm is adhered to, namely, that all the elements that define the SMART survey are encapsulated in the Database *data*.
In principle, it should be possible for ``mwa_search`` not to know *anything* about the SMART processing pipeline; it should all be inferable from the contents of the Database itself.

This lofty goal, however, has not yet been reached; for now, the burden of *defining* the SMART processing pipeline still rests on **this** repo (and in particular, in **this** documentation).
But instead of trying to document the use of the Database generally, this documentation describes how **the second pass** of the SMART survey can be defined, with the hope that it may serve as an example, or a template, of how future "passes" (or indeed, other surveys) might be similarly constructed.

Template for NextFlow processes
-------------------------------

NextFlow processes map to the various stages along the SMART processing pipeline.
The details of each process, however, depend on which processing pass is being conducted, which algorithms have been pre-selected *for* that pass, and which implementations of those algorithms have been decided on (i.e. which software, and which versions of said software).
The collection of algorithms and implementations that have been decided on for that pass actually *define* that pass, and all of this information is precisely what is to be stored in v2.0 (and later) of the Database itself.

Therefore, the template for each NextFlow pipeline involves

1. A call to the Database to retreive meta-information about the process to be executed
2. The execution of the process itself
3. Another call to the Database to populate it with a record of the processing work that has just been completed.

.. graphviz::

   digraph G {
     rankdir="LR";
     read [label="Read from database"];
     process [label="Process execution"];
     write [label="Write to database"];

     read -> process -> write;
   }

Scripts for both reading from and writing to the Database are kept in the ``scripts/smart_database`` folder, with scripts for reading having the name ``get_*`` and scripts for writing having the name ``set_*``.
Other scripts in the ``scripts/smart_database`` folder are auxilliary scripts, e.g. for handling the connection to the Database.

Defining SMART processing passes in the Database
-------------------------------------------------

In this section, we walk through an example of how to use the Database to define (a small number of components of) the second pass survey.
This is just an example: some details laid out in this section may become obsolete as the details about the second pass evolve.
Thus, in cases where the Database diverges from this documentation, the Database should be considered definitive.

There is (by design) no public-facing API for editing the tables that relate to defining the survey passes (also known as "survey chapters").
Database administrators can (and should) edit these fields directly using the Django admin site (``http://.../admin/``).

Survey chapters
^^^^^^^^^^^^^^^

New survey chapters can be created by entering new rows in the ``SurveyChapter`` table:

.. list-table:: SurveyChapter table
   :widths: 25 75
   :header-rows: 1

   * - Name
     - Algorithm settings
   * - first_pass
     - ...
   * - second_pass
     - ...

The ``Algorithm settings`` field is a many-to-many relation, and the values that will eventually go here collectively *define* the second pass.
This can be initially left blank and only populated after the algorithm settings themselves have been defined.

Algorithms
^^^^^^^^^^

The ``Algorithm`` table can be used to group blocks of processing tasks together (or really, to *define* them).
**For SMART, we interpret ``Algorithms`` to be the Database analogue of the NextFlow process, and specify that there must be a one-to-one correspondence between the two.**

For example, both the first and second passes require a process for generating a list of pointings for the tied-array beams that are used either for the initial search or for follow-up searches.
The script that is used to generate this list is ``scripts/mwa_search/grid.py`` (included in this repo), which requires several input parameters to be defined.
Suppose that this script is called from two NextFlow processes called "search_pointings" and "followup_pointings".
Then, we would add two entries to the ``Algorithm`` table as follows:

.. list-table:: Algorithm table
   :widths: 25 75
   :header-rows: 1

   * - Name
     - Description
   * - search_pointings
     - | Generate a list of pointings for forming tied-array beams
       | that tile the FoV of a given observation
   * - followup_pointings
     - | Generate a list of pointings for forming tied-array beams
       | specifically for following up individual candidates

These algorithms must now be defined.

Algorithm parameters
^^^^^^^^^^^^^^^^^^^^

Algorithms, conceptually, are really just lists of parameters, and this is embodied in the Database as a many-to-many relation between the ``Algorithm`` table and the ``AlgorithmParameter`` table.
Since we have mandated (for SMART) that "algorithms" have a one-to-one correspondence to NextFlow processes, "algorithm parameters" should map to options that are needed *by* those processes.
In this example, this will necessarily include options that get passed to the ``grid.py`` script, which we therefore add to the ``AlgorithmParameter`` table:

.. list-table:: AlgorithmParameter table
   :widths: 25 25 50
   :header-rows: 1

   * - Name
     - Algorithms
     - Description
   * - deg_fwhm
     - | followup_pointings
       | search_pointings
     - The FWHM of the tied-array beam at zenith in degrees
   * - fraction
     - | followup_pointings
       | search_pointings
     - | Fraction of the full width half maximum to use as the
       | distance between beam centres
   * - n_pointings
     - | followup_pointings
       | search_pointings
     - Number of pointings per output file

Algorithm settings
^^^^^^^^^^^^^^^^^^

Note that the algorithm parameters have not yet been assigned values.
This is because although the parameters themselves are defined by the algorithms, the specific values that are used depend on the survey chapter.
Linking specific parameter values to different survey chapters is the job of the ``AlgorithmSetting`` table, which effectively acts as a join table between ``AlgorithmParameter`` and ``SurveyChapter``.
However, the many-to-many relation between ``SurveyChapter`` and ``AlgorithmSetting`` is defined in the Database as a ``ManyToManyField`` in the ``SurveyChapter`` model, so assigning specific values to algorithm parameters is a two-step process:

1. Create entries in the ``AlgorithmSetting`` table that define the values,
2. Add those entries to the "Algorithm settings" field of the appropriate row in the ``SurveyChapter`` table.

For example, suppose that both first and second passes used the same parameter values except that the first pass uses a wider spacing of pointings than the second pass, which is reflected by a different value for the "fraction" parameter (1.2 for first pass, 0.9 for second pass).
We might therefore add the following parameters to the ``AlgorithmSettings`` table:

.. list-table:: AlgorithmSetting table (only the fields used in this example are shown)
   :widths: 25 75
   :header-rows: 1

   * - Algorithm parameter
     - Value
   * - deg_fwhm
     - 0.3
   * - fraction
     - 0.9
   * - fraction
     - 1.2
   * - n_pointings
     - 1080

These settings will now be available for selection within the ``SurveyChapter`` table.
Thus, we now update the rows for the first and second pass accordingly:

.. list-table:: SurveyChapter table
   :widths: 25 75
   :header-rows: 1

   * - Name
     - Algorithm settings
   * - first_pass
     - | deg_fwhm = 0.3
       | fraction = 1.2
       | n_pointings = 1080
   * - second_pass
     - | deg_fwhm = 0.3
       | fraction = 0.9
       | n_pointings = 1080

.. note::
   The Django admin interface allows for the "dynamic" adding of entries of fields by using the "+" button, which can streamline the data-entry process.

Using Database-defined parameters in NextFlow processes
-------------------------------------------------------

The tool for retrieving defined algorithm settings from the Database is the ``scripts/smart_database/get_algorithm_settings.py`` script.
It takes as required inputs a survey chapter name and the name of an algorithm, and will return all algorithm settings that match those two constraints.

The script can be called from the command line, in which case the results of the query are written to stdout, or it can be imported as a python module, in which case the ``get_algorithm_settings()`` function returns a list of dictionaries whose keys are ``algorithm_parameter__name``, ``value``, and ``config_file`` (discussed below [TODO: add link to section]).
Calling the script from the command line is necessary when the NextFlow process that depends on those values involves calls to external software.
However, if the process uses scripts in ``mwa_search`` (such as ``grid.py``), then the scripts themselves may import ``get_algorithm_settings.py`` directly and use the dictionary is later processing.
Using the latter method is a matter of taste.

.. note::

   This script and all other scripts that interface with the database require a "token" and a "base url" to be granted authorisation access to the Database (hosted at "base url").
   See the ``--help`` docstring of these scripts for more information.

Example
^^^^^^^

.. code-block::
   :caption: An example of command line usage

   $ get_algorithm_settings.py --token=$SMART_TOKEN --base_url=$SMART_BASE_URL first_pass followup_pointings --pretty
   deg_fwhm 0.3
   fraction 1.2
   n_pointings 1080

   $ get_algorithm_settings.py --token=$SMART_TOKEN --base_url=$SMART_BASE_URL second_pass followup_pointings --pretty
   deg_fwhm 0.3
   fraction 0.9
   n_pointings 1080

Handling variable parameter settings
------------------------------------

In some cases, the input parameters needed by the NextFlow processes depend not only on certain pre-decided and fixed settings, but also on values that change from processing job to processing job.
For example, one difference between the first and second passes is amount of observational data that is processed, which in turn changes the number of tied-array beam pointings that are used.
The actual parameters that ``grid.py`` is expecting include ``--begin`` and ``--end`` times, which not only differ from survey chapter to survey chapter, but also from observation to observation.
The ``AlgorithmSetting`` table is only designed for fixed values, so in this case the best approach is to invent a custom parameter that provides a high-level distinction between the various possibilities, and to implement the logic to interpret those values for specific processes locally.

In this example, we might define a pair of parameters as follows:

.. list-table:: AlgorithmParameter table
   :widths: 25 25 50
   :header-rows: 1

   * - Name
     - Algorithms
     - Description
   * - skip_nseconds
     - | search_pointings
       | followup_pointings
     - Skip the first N seconds of the observation
   * - process_nseconds
     - | search_pointings
       | followup_pointings
     - Process only N seconds of the observation

These two parameters would very likely also be used in other algorithms apart from the two listed here (e.g. for the processes that actually *do* the beamforming), but this example is just limited to the algorithms already introduced in the previous sections.

With these defined parameters, the ``AlgorithmSetting`` table would need the following values defined for the first pass (which uses the first ten minutes of each observation) and second pass (which uses the whole observation):

.. list-table:: AlgorithmSetting table
   :widths: 25 75
   :header-rows: 1

   * - Algorithm parameter
     - Value
   * - skip_nseconds
     - 0
   * - process_nseconds
     - 600
   * - process_nseconds
     - all

These are then tied to survey chapters in the usual way:

.. list-table:: SurveyChapter table
   :widths: 25 75
   :header-rows: 1

   * - Name
     - Algorithm settings
   * - first_pass
     - | skip_nseconds = 0
       | process_nseconds = 600
   * - second_pass
     - | skip_nseconds = 0
       | process_nseconds = all

The actual values that need to be passed into ``grid.py`` must now be worked out from these definitions inside the NextFlow script that calls ``grid.py``.
However, the ``--begin`` and ``--end`` values also depend on which observation is being "gridded", and while the observation itself is necessarily provided by the user, the information needed to calculate these values must *also* be retrieved from the database (in this case, the "start_time" and "stop_time" fields of the ``Observation`` table).
