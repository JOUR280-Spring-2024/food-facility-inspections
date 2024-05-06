import requests
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column, Integer, String
import pendulum
import json

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}

engine = create_engine('sqlite:///food_inspection.db')
metadata = MetaData()

facility_table = Table('Facility', metadata,
                       Column('facilityId', String, primary_key=True),
                       Column('name', String),
                       Column('address_and_zip', String),
                       Column('last_inspection_date', String),
                       Column('notice_placard', String)
                       )
metadata.create_all(engine)

valid_data = False
page = 0

while not valid_data:
    url = f'https://champaign-il.healthinspections.us/API/index.cfm/facilities/{page}/0'
    print(url)
    response = requests.get(url, headers=headers)
    facility_data = response.json()
    if facility_data == []:
        valid_data = True
    else:
        for facility in facility_data:
            with engine.connect() as connection:
                stmt = sqlite_insert(facility_table).values(facilityId=facility['id'],
                                                              name=facility['name'],
                                                              address_and_zip=facility['mapAddress'].strip(),
                                                              last_inspection_date=pendulum.from_format(facility['columns']['1'][22:], "MM-DD-YYYY").to_date_string(),
                                                              notice_placard=facility['columns']['2'][16:])
                do_update_stmt = stmt.on_conflict_do_update(index_elements=['facilityId'], set_=dict(
                    name=stmt.excluded.name,
                    address_and_zip=stmt.excluded.address_and_zip,
                    last_inspection_date=stmt.excluded.last_inspection_date,
                    notice_placard=stmt.excluded.notice_placard
                ))
                connection.execute(do_update_stmt)
                connection.commit()
        page += 1

inspection_table = Table('Inspection', metadata,
                       Column('inspectionId', Integer, primary_key=True),
                       Column('facilityId', String),
                       Column('inspection_date', String),
                       Column('inspection_result', String),
                       Column('inspection_purpose', String),
                       Column('violations', String)
                       )
metadata.create_all(engine)

with engine.connect() as connection:
    select_query = facility_table.select()
    result = connection.execute(select_query)
    for row in result:
        url = f'https://champaign-il.healthinspections.us/API/index.cfm/inspectionsData/{row[0]}'
        print(url)
        response = requests.get(url, headers=headers)
        inspection_data = response.json()
        for inspection in inspection_data:
            stmt = sqlite_insert(inspection_table).values(inspectionId=inspection['inspectionId'],
                                                        facilityId=inspection['facilityId'],
                                                        inspection_date = pendulum.from_format(inspection['columns']['0'][17:], "MM-DD-YYYY").to_date_string(),
                                                        inspection_result = inspection['inspectionResult'][16:],
                                                        inspection_purpose = inspection['inspectionPurpose'][20:],
                                                        violations = json.dumps(inspection['violations']))
            do_update_stmt = stmt.on_conflict_do_update(index_elements=['inspectionId'], set_=dict(
                    facilityId=stmt.excluded.facilityId,
                    inspection_date=stmt.excluded.inspection_date,
                    inspection_result=stmt.excluded.inspection_result,
                    inspection_purpose=stmt.excluded.inspection_purpose,
                    violations=stmt.excluded.violations
                ))
            connection.execute(do_update_stmt)
        connection.commit()