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

## Images

Conceptual database design:
![Screenshot_20230201_181635](https://user-images.githubusercontent.com/113461257/217527908-6a600cfa-b3d9-4878-969d-4e4bdc1ca584.png)


Logical database design:
<img width="465" alt="Screenshot_20230202_104833" src="https://user-images.githubusercontent.com/113461257/217527760-1d938049-03e6-46e5-80ae-592d78e0eecf.png">


Physical database design:
<img width="471" alt="Screenshot_20230202_110739" src="https://user-images.githubusercontent.com/113461257/217527842-a436a793-b414-4941-9817-9dd541af02fb.png">



gold-dim.bike:
![gold-bike id](https://user-images.githubusercontent.com/113461257/217528328-683fcba8-09f1-4484-b19f-f48949483dff.png)


gold-dim.date:
![gold-date dim](https://user-images.githubusercontent.com/113461257/217528431-3e511320-600f-4c33-957a-0d36c9f5db98.png)


gold-fact.trip:
![gold-fact trip](https://user-images.githubusercontent.com/113461257/217528522-b87bbe98-f098-4a62-94b0-97beaa0f72a3.png)


gold-fact.payment:
![Gold-payment fact](https://user-images.githubusercontent.com/113461257/217528648-c0a9cb1e-4b92-46f7-8b61-48fad7fe1ef2.png)


gold-dim.rider:
![gold-rider dim](https://user-images.githubusercontent.com/113461257/217528707-a6140107-0635-439c-ba1e-a9767e726fb6.png)


gold-dim.station:
![gold-station dim](https://user-images.githubusercontent.com/113461257/217528770-7bb07a88-5ab0-4761-aa7f-4a062e44ff18.png)


gold-dim.time:
![gold-time dim](https://user-images.githubusercontent.com/113461257/217528971-76f95cc5-1fd4-4542-ae48-0ea39bc73d7b.png)



Successful workflow:
<img width="687" alt="Screenshot_20230208_113338" src="https://user-images.githubusercontent.com/113461257/217529086-1a53fd69-9735-4e10-a0f8-b4a7354e3f72.png">


