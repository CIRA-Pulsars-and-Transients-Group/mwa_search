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
But instead of trying to document the use of the Database generally, this documentation describes how **the second pass** of the SMART survey is defined, with the hope that it may serve as an example, or a template, of how future "passes" (or indeed, other surveys) might be similarly constructed.

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
Database administrators can (and should) edit these fields directly using the Django admin site (.../admin/).

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

Defining algorithms
^^^^^^^^^^^^^^^^^^^

The ``Algorithm`` table can be used to group blocks of processing tasks together (or really, to *define* them).
**For SMART, we interpret ``Algorithms`` to be the Database analogue of the NextFlow process, and specify that there must be a one-to-one correspondence between the two.**

For example, both the first and second passes use a process for generating a list of pointings for the tied-array beams that are used for the initial search.
The script that is used to generate this list is ``scripts/mwa_search/grid.py`` (included in this repo), which requires several input parameters to be defined.
Suppose that this script is

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
   * - ...
     - ...

Algorithms, conceptually, are really lists of parameters, and this is embodied in the Database as a many-to-many relation between the ``Algorithm`` table and the ``AlgorithmParameter`` table.
Parameters for the above-listed algorithms are

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
     - | Number of pointings per output file
   * - ...
     - ...
     - ...

Note that the algorithm parameters have not yet been assigned values.
This is because although the parameters themselves are defined by the algorithms, the specific values that are used depend on the survey chapter.
Linking specific parameter values to different survey chapters is the job of the ``AlgorithmSetting`` table, which effectively acts as a join table between ``AlgorithmParameter`` and ``SurveyChapter``.
However, the many-to-many relation between ``SurveyChapter`` and ``AlgorithmSetting`` is defined in the Database as a ``ManyToManyField`` in the ``SurveyChapter`` model, so implementing specific values for algorithm parameters (via the Django admin interface) involves two steps: (1) creating entries in the ``AlgorithmSetting`` table that define the values, and (2) adding those entries to the appropriate field in the ``SurveyChapter`` table.

.. list-table:: AlgorithmSetting table (only the fields used in this example are shown)
   :widths: 25 75
   :header-rows: 1

   * - Algorithm parameter
     - Value
   * - deg_fwhm
     - 0.3
   * - fraction
     - 0.9
   * - n_pointings
     - 1080

.. list-table:: SurveyChapter table
   :widths: 25 75
   :header-rows: 1

   * - Name
     - Algorithm settings
   * - second_pass
     - | deg_fwhm = 0.3
       | fraction = 0.9
       | n_pointings = 1080
