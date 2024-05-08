# Sheriff's Office Daily Report

This scraper extracts data from the [food establishment inspections](https://www.c-uphd.org/food-inspection-reports.html) in Champaign County.

We extract all the data about food facilities and inspections, but not the detailed reports ([sample](https://champaign-il.healthinspections.us/_templates/90/Food_Establishment_Inspection/_report_full.cfm?inspectionID=332845&domainid=90)). 

The data is stored as a SQLite database named `food_inspection.sqlite`.

The code can be run every day to add new records to the database. Previous records are retained and new
records are added.

If you end up using this database in a news report, please give credit to Jenny Lin.