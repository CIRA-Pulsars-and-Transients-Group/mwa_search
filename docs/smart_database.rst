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

Defining the second pass of SMART in the Database
-------------------------------------------------

The second pass itself is represented abstractly by a row in the ``SurveyChapter`` table:

.. list-table:: SurveyChapter table
   :widths: 25 75
   :header-rows: 1

   * - Name
     - Algorithm settings
   * - second_pass
     - ...

(NB: all entries are case-sensitive.)
The ``Algorithm settings`` field is a many-to-many relation, and the values that go here collectively define the second pass, and are introduced in the following subsections.

Second pass algorithms
^^^^^^^^^^^^^^^^^^^^^^

The ``Algorithm`` table can be used to group blocks of processing tasks together (or really, to *define* them).
For SMART, we interpret ``Algorithms`` to be the Database analogue of the NextFlow process, and specify that there must be a one-to-one correspondence between the two.

Algorithms used in the second pass are the following:

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
