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

The physical database design was what I had to implement within the Gold layer.

I had to start off with a basic schema with no data type changes to prevent any loss with the data when being transfered to the Bronze layer.

The second schema that I created can now change the data type to be in accordace with the original data.

Finaly the third schema had to follow the physical database design of the star schema created

##Images

Conceptual database design:
![Screenshot_20230201_181635](https://user-images.githubusercontent.com/113461257/217527908-6a600cfa-b3d9-4878-969d-4e4bdc1ca584.png)

Logical database design:
<img width="465" alt="Screenshot_20230202_104833" src="https://user-images.githubusercontent.com/113461257/217527760-1d938049-03e6-46e5-80ae-592d78e0eecf.png">

Physical database design:
<img width="471" alt="Screenshot_20230202_110739" src="https://user-images.githubusercontent.com/113461257/217527842-a436a793-b414-4941-9817-9dd541af02fb.png">

