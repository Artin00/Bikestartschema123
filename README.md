# Bikestartschema123 
^ Photos are to be ordered chronologically from where '(!)' is shown.

This project is to try and help a bike sharing program in Chichago, Illinois USA. For their bikeshare solution, I was tasked to create a Star schema to try and help 
answer some of their business outcomes.

A general task was to:

* Design a Star Schema that aligns with the business outcomes.
* Import the data in the dbfs of Azure Databricks and use Delta Lake to create the different tables in different directories.
* Transform the data into the star schema and import it in the Gold directory.

## Setting up the Schemas

To start of the project I had to figure out what sort of star schema would work to contain all the primary and foreign keys within fact tables and which columns will need to be rearranged or removed.

We had to create :

* Conceptual database design (!)
* Logical database design (!)
* Physical database design (!)

The one that was used was the physical database design that we had to implement within the Gold layer.


