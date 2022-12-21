.. _smart_database:

SMART Database
==============

Relationship to mwa_search
--------------------------

The SMART search pipeline as implemented in `mwa_search` (for the second pass and beyond) is inextricably intertwined with the `SMART Database and Webapp <https://github.com/ADACS-Australia/SS2020A-RBhat>`_ (private GitHub repository), which hereafter will be referred to as "the Database".
Versions of the Database from v2.0 onward have been designed with generality in mind, and can potentially be used by pulsar surveys other than SMART, with all the elements that *define* the SMART survey being encapsulated by the **data** in the Database, rather than in the **structure** of the Database.

v4.0 of `mwa_search` is the first version that will use the new Database structure defined in v2.0 (of the Database).
And just as the Database is on a trajectory towards more general usage (outside of SMART), so too are future versions of `mwa_search` destined to be more general as well.
This is possible to the extent to which the above-mentioned paradigm is adhered to, namely, that all the elements that define the SMART survey are encapsulated in the Database *data*.
In principle, it should be possible for `mwa_search` not to know *anything* about the SMART processing pipeline; it should all be inferable from the contents of the Database itself.

This lofty goal, however, has not been reached; for now, the burden of *defining* the SMART processing pipeline still rests on **this** repo (an in particular, in **this** documentation).
But instead of trying to document the use of the Database generally, this documentation describes how **the second pass** of the SMART survey is defined, with the hope that it may serve as an example, or a template, of how future "passes" (or indeed, other surveys) might be similarly constructed.
